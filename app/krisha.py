import re
from dataclasses import asdict, dataclass
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.config import Settings

SEARCH_URL = "https://krisha.kz/arenda/kvartiry/almaty/"
SOURCE_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; KrishaRentalTracker/1.0)"}
RENTED_NOTICE = re.compile(
    r"^(?:квартира\s+)?(?:уже\s+)?(?:сдана|заселена)\b(?!\s+в\s+эксплуатацию)"
    r"|^(?:жильцы|арендаторы)\s+найдены\b"
    r"|^(?:объявление\s+)?не\s*актуально\b|^уже\s+заселились\b",
    re.IGNORECASE,
)
# Match actual offers to share, rather than excluding 'без подселения'.
UNSUITABLE = re.compile(
    r"(?<!без )(?<!не )\bподселен\w*|\b(?:ищ[уе]м?|нуж[еён]н?а?)\s+сосед\w*"
    r"|\b(?:сдам|сда[её]тся|сда[её]м)\s+комнат[ау]\b"
    r"|\bкойко[ -]?мест\w*|\bобщежит\w*|\bгостинк\w*"
    r"|\bпосут[оа]ч\w*|\bпосуточ\w*|\bпо\s+часам\b"
    r"|\bна\s+(?:[1-9]|один|два|три)\s+(?:дн[яей]|недел\w*)\b",
    re.IGNORECASE,
)


class SourceError(Exception):
    pass


def has_rented_notice(text: str) -> bool:
    # A direct statement, not mentions of other offers or questions/negations.
    for sentence in re.findall(r"[^.!?\n]+[.!?]?", text):
        if not sentence.rstrip().endswith("?") and RENTED_NOTICE.match(sentence.strip()):
            return True
    return False


def parse_availability(html: str, listing_id: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for node in soup.select("script, style, .a-card"):
        node.decompose()
    canonical = soup.select_one('link[rel="canonical"]')
    expected = f"https://krisha.kz/a/show/{listing_id}"
    if canonical is None or str(canonical.get("href", "")).split("?")[0].rstrip("/") != expected:
        raise SourceError("Krisha: cannot verify advert identity; keeping previous availability")
    # Archive notices can remain visible while the advert itself is still readable.
    for marker in soup.find_all(string=re.compile(r"^\s*В архиве\s*$", re.I)):
        parent = marker.parent
        for _ in range(3):
            if parent is None:
                break
            text = parent.get_text(" ", strip=True)
            if len(text) > 500:
                break
            if "объявление может быть неактуальным" in text.casefold():
                return "archived"
            parent = parent.parent
    heading = soup.select_one("h1")
    if heading is None or not heading.get_text(strip=True):
        raise SourceError("Krisha: unrecognized advert page; keeping previous availability")
    description = soup.select_one('.offer__description, [itemprop="description"]')
    for node in (heading, description):
        if node is not None and has_rented_notice(node.get_text(" ", strip=True)):
            return "rented"
    return "active"


async def check_availability(client: httpx.AsyncClient, listing_id: str) -> str:
    if not re.fullmatch(r"\d+", listing_id):
        raise ValueError("Invalid listing ID")
    try:
        response = await client.get(f"https://krisha.kz/a/show/{listing_id}", headers=SOURCE_HEADERS)
        # A redirect to search/home is not proof that the particular advert was removed.
        if str(response.url).split("?")[0].rstrip("/") != f"https://krisha.kz/a/show/{listing_id}":
            raise SourceError("Krisha: advert redirected; keeping previous availability")
        if response.status_code in (404, 410):
            return "removed"
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise SourceError(f"Krisha availability check failed: {type(exc).__name__}") from None
    return parse_availability(response.text, listing_id)


@dataclass(frozen=True)
class Listing:
    id: str
    title: str
    price: int
    area: float | None
    address: str
    description: str
    url: str
    has_photo: bool
    owner: bool
    monthly: bool
    city: str

    def to_dict(self):
        return asdict(self)


def parse_listings(html: str) -> tuple[list[Listing], bool]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".a-card[data-id]")
    if not cards:
        # An explicit empty search is valid. A challenge or changed markup isn't.
        if soup.select_one(".a-list-empty, .search-empty, .a-search-empty"):
            return [], False
        raise SourceError("Krisha: no listing cards; possible block or changed markup")
    listings = []
    for card in cards:
        title_node = card.select_one(".a-card__title")
        price_node = card.select_one(".a-card__price")
        if title_node is None or price_node is None:
            continue
        title = title_node.get_text(" ", strip=True)
        price_text = price_node.get_text(" ", strip=True)
        price_match = re.search(r"([\d\s\xa0\u202f]+)\s*₸", price_text)
        href = str(title_node.get("href", ""))
        advert_id = str(card.get("data-id", ""))
        if not price_match or not re.fullmatch(r"\d+", advert_id):
            continue
        if href != f"/a/show/{advert_id}":
            continue
        area_match = re.search(r"(\d+(?:[.,]\d+)?)\s*м[²2]", title)

        def content(selector):
            node = card.select_one(selector)
            return node.get_text(" ", strip=True) if node else ""

        image = card.select_one(".a-image__img")
        image_source = str(image.get("src", "")) if image else ""
        listings.append(
            Listing(
                id=advert_id,
                title=title,
                price=int(re.sub(r"\D", "", price_match.group(1))),
                area=float(area_match.group(1).replace(",", ".")) if area_match else None,
                address=content(".a-card__subtitle"),
                description=content(".a-card__text-preview"),
                url=urljoin(SEARCH_URL, href),
                has_photo=bool(image_source and "nophoto" not in image_source),
                owner=card.select_one(".user-owner, .label-user-owner") is not None,
                monthly="за месяц" in price_text.casefold(),
                city=content(".a-card__stats-item"),
            )
        )
    if not listings:
        raise SourceError("Krisha: cards found but parsing failed")
    return listings, soup.select_one(".paginator__btn--next") is not None


def is_match(listing: Listing, settings: Settings) -> bool:
    return bool(
        re.match(r"^1\s*[-–]?\s*комнатная\s+квартира\b", listing.title, re.I)
        and listing.city.casefold() == "алматы"
        and listing.monthly
        and settings.min_price <= listing.price <= settings.max_price
        and listing.area is not None
        and listing.area >= settings.min_area
        and (not settings.require_photo or listing.has_photo)
        and (not settings.owners_only or listing.owner)
        and not UNSUITABLE.search(listing.description)
        and not has_rented_notice(listing.description)
    )


async def fetch_listings(client: httpx.AsyncClient, settings: Settings) -> list[Listing]:
    found: dict[str, Listing] = {}
    for page in range(1, settings.search_pages + 1):
        try:
            response = await client.get(
                SEARCH_URL,
                params={
                    "das[live.rooms]": 1,
                    "das[price][from]": settings.min_price,
                    "das[price][to]": settings.max_price,
                    "sort_by": "add-date",
                    "page": page,
                },
                headers=SOURCE_HEADERS,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise SourceError(f"Krisha request failed: {type(exc).__name__}") from None
        listings, has_next = parse_listings(response.text)
        found.update({listing.id: listing for listing in listings})
        if not has_next:
            break
    return list(found.values())
