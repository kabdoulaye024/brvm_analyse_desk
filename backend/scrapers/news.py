"""
News scraper for BRVM Trading Desk.
Cascade: brvm.org → richbourse.com
Returns list of dicts: title, url, published_at, source, summary.
"""
import logging
import re
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    import cloudscraper
    _scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
except Exception:
    _scraper = None

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

RICHBOURSE_BASE = "https://www.richbourse.com"
BRVM_ORG_BASE = "https://www.brvm.org"


def _safe_get(url: str, timeout: int = 20, **kwargs) -> Optional[requests.Response]:
    """GET with cloudscraper for richbourse (Cloudflare), plain requests for others."""
    try:
        if "richbourse.com" in url and _scraper:
            resp = _scraper.get(url, timeout=timeout, **kwargs)
        else:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, verify=False, **kwargs)
        if resp.status_code == 200:
            return resp
        logger.warning(f"GET {url}: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"GET {url}: {e}")
    return None


# ══════════════════════════════════════════════════════════════
# BRVM.ORG NEWS
# ══════════════════════════════════════════════════════════════

def _fetch_brvm_org_news(limit: int = 30) -> list[dict]:
    """
    Scrape news headlines from brvm.org.
    Tries /fr/actualites then /en/news as fallback.
    Parses <article>, <div class="views-row">, and generic <a> links.
    """
    urls_to_try = [
        f"{BRVM_ORG_BASE}/fr/actualites",
        f"{BRVM_ORG_BASE}/en/news",
        f"{BRVM_ORG_BASE}/fr/news",
        f"{BRVM_ORG_BASE}/en/actualites",
    ]

    resp = None
    used_url = None
    for url in urls_to_try:
        resp = _safe_get(url, timeout=20)
        if resp:
            used_url = url
            break

    if not resp:
        logger.warning("brvm.org news: all URLs failed")
        return []

    items = []
    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        # Strategy 1: <article> elements
        articles = soup.find_all("article")

        # Strategy 2: Drupal-style views-row divs
        if not articles:
            articles = soup.find_all("div", class_=re.compile(r"views-row|field-item|news-item|actualite", re.I))

        # Strategy 3: list-group items or generic news containers
        if not articles:
            articles = (
                soup.select(".list-group-item") or
                soup.find_all(class_=re.compile(r"article|post|news|actualite", re.I))
            )

        if articles:
            for art in articles[:limit * 2]:
                # Find title
                title_el = (
                    art.find(["h1", "h2", "h3", "h4", "h5"]) or
                    art.find(class_=re.compile(r"title|titre|heading", re.I))
                )
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if len(title) < 10:
                    continue

                # Find link — first check the title element itself, then any <a>
                link_el = title_el.find("a", href=True) if title_el else None
                if not link_el:
                    link_el = art.find("a", href=True)
                href = ""
                if link_el:
                    href = link_el.get("href", "")
                    if href.startswith("/"):
                        href = BRVM_ORG_BASE + href
                    elif not href.startswith("http"):
                        href = BRVM_ORG_BASE + "/" + href.lstrip("/")

                # Find date
                date_el = art.find(
                    ["time", "span", "div"],
                    class_=re.compile(r"date|time|pub|posted|submitted", re.I)
                )
                if not date_el:
                    date_el = art.find("time")
                pub_date = ""
                if date_el:
                    pub_date = date_el.get("datetime", "") or date_el.get_text(strip=True)

                # Find summary
                summary_el = art.find("p")
                summary = summary_el.get_text(strip=True)[:300] if summary_el else ""

                items.append({
                    "title": title,
                    "url": href,
                    "summary": summary,
                    "published_at": pub_date,
                    "source": "brvm.org",
                })
        else:
            # Fallback: scan all <a> links that look like news
            links = soup.find_all(
                "a",
                href=re.compile(r"actualite|article|news|communique|publication", re.I)
            )
            for lnk in links[:limit * 2]:
                title = lnk.get_text(strip=True)
                if len(title) < 15:
                    continue
                href = lnk.get("href", "")
                if href.startswith("/"):
                    href = BRVM_ORG_BASE + href
                elif not href.startswith("http"):
                    href = BRVM_ORG_BASE + "/" + href.lstrip("/")

                parent = lnk.find_parent()
                pub_date = ""
                if parent:
                    date_el = parent.find(
                        class_=re.compile(r"date|time|pub|posted|submitted", re.I)
                    )
                    if not date_el:
                        date_el = parent.find("time")
                    if date_el:
                        pub_date = date_el.get("datetime", "") or date_el.get_text(strip=True)

                items.append({
                    "title": title,
                    "url": href,
                    "summary": "",
                    "published_at": pub_date,
                    "source": "brvm.org",
                })

        # Deduplicate by title
        seen = set()
        unique = []
        for it in items:
            key = it["title"].lower().strip()
            if key not in seen and len(it["title"]) > 10:
                seen.add(key)
                unique.append(it)

        logger.info(f"brvm.org news: fetched {len(unique)} items from {used_url}")
        return unique[:limit]

    except Exception as e:
        logger.warning(f"brvm.org news parse error: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# RICHBOURSE NEWS
# ══════════════════════════════════════════════════════════════

def _fetch_richbourse_news(limit: int = 30) -> list[dict]:
    """
    Scrape latest news from richbourse.com.
    Article links follow the pattern /common/actualite/details/{date}-{slug}.
    Also tries the communiqués page.
    """
    items = []
    seen = set()

    for url in [
        f"{RICHBOURSE_BASE}/common/actualite/index",
        f"{RICHBOURSE_BASE}/common/publication/index",
    ]:
        resp = _safe_get(url, timeout=20)
        if not resp:
            continue
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            # Find all <a> whose href contains 'actualite/details' or 'publication/details'
            links = soup.find_all("a", href=re.compile(
                r"/(actualite|publication|communique|rapport)/details?/", re.I))
            for lnk in links:
                title = lnk.get_text(strip=True)
                if len(title) < 15 or title.lower() in seen:
                    continue
                seen.add(title.lower())
                href = lnk.get("href", "")
                if href.startswith("/"):
                    href = RICHBOURSE_BASE + href
                # Try to extract date from URL slug: /details/DD-MM-YYYY-slug
                pub_date = ""
                m = re.search(r"/(\d{2}-\d{2}-\d{4})-", href)
                if m:
                    d, mo, y = m.group(1).split("-")
                    pub_date = f"{y}-{mo}-{d}"
                # Try to find date in nearby element
                if not pub_date:
                    parent = lnk.find_parent()
                    for _ in range(3):
                        if parent:
                            date_el = parent.find(
                                class_=re.compile(r"date|time|pub", re.I)) or parent.find("time")
                            if date_el:
                                pub_date = (date_el.get("datetime") or
                                            date_el.get_text(strip=True))
                                break
                            parent = parent.find_parent()
                items.append({
                    "title": title,
                    "url": href,
                    "summary": "",
                    "published_at": pub_date,
                    "source": "richbourse",
                })
                if len(items) >= limit:
                    break
        except Exception as e:
            logger.debug(f"richbourse news {url}: {e}")
        if len(items) >= limit:
            break

    logger.info(f"richbourse news: fetched {len(items)} items")
    return items[:limit]


# ══════════════════════════════════════════════════════════════
# MAIN CASCADE — fetch_news()
# ══════════════════════════════════════════════════════════════

def fetch_news(limit: int = 30) -> list[dict]:
    """
    Fetch BRVM-related news via cascade:
      1. brvm.org (primary — official exchange announcements)
      2. richbourse.com (secondary — market commentary)

    Returns a deduplicated list of up to `limit` news items, each with:
      title, url, published_at, source, summary.
    """
    all_items: list[dict] = []
    seen_titles: set[str] = set()

    def _add(items: list[dict]) -> None:
        for it in items:
            key = it["title"].lower().strip()
            if key and key not in seen_titles:
                seen_titles.add(key)
                all_items.append(it)

    # 1. brvm.org — runs first
    brvm_items = _fetch_brvm_org_news(limit=limit)
    _add(brvm_items)
    logger.info(f"fetch_news cascade: brvm.org contributed {len(brvm_items)} items")

    # 2. richbourse — fills up to limit
    if len(all_items) < limit:
        rb_items = _fetch_richbourse_news(limit=limit)
        _add(rb_items)
        logger.info(f"fetch_news cascade: richbourse contributed {len(rb_items)} items")

    result = all_items[:limit]
    logger.info(f"fetch_news: returning {len(result)} total items")
    return result
