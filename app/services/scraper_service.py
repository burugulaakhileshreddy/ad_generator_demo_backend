import re
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from app.services.storage_service import download_images, download_logo


# ---------------------------------------------------------
# FAST MODE SETTINGS
# ---------------------------------------------------------

HOMEPAGE_IMAGE_TARGET = 18
MAX_INTERNAL_PAGES_FAST = 1


# ---------------------------------------------------------
# URL NORMALIZATION
# ---------------------------------------------------------

def normalize_input_url(url: str) -> str:
    if not url:
        raise ValueError("URL is required")

    cleaned = url.strip()

    if not cleaned:
        raise ValueError("URL is required")

    if not cleaned.startswith(("http://", "https://")):
        cleaned = "https://" + cleaned

    parsed = urlparse(cleaned)

    if not parsed.netloc:
        raise ValueError(f"Invalid URL: {url}")

    return cleaned


# ---------------------------------------------------------
# BUSINESS TEXT EXTRACTION
# ---------------------------------------------------------

def extract_business_text(soup):
    texts = []

    for tag in soup.find_all(["h1", "h2", "h3", "p"]):
        text = tag.get_text(strip=True)

        if len(text) < 40:
            continue

        lower = text.lower()

        if any(x in lower for x in [
            "cookie", "privacy", "login", "subscribe", "terms"
        ]):
            continue

        texts.append(text)

    return " ".join(texts[:12])


# ---------------------------------------------------------
# LOGO DETECTION
# ---------------------------------------------------------

def detect_logo(page, soup):
    return page.evaluate("""
    () => {
        const imgs = document.querySelectorAll("img");

        for (const img of imgs) {
            const alt = (img.alt || "").toLowerCase();
            const cls = (img.className || "").toLowerCase();
            const src = (img.src || "").toLowerCase();

            if (
                alt.includes("logo") ||
                cls.includes("logo") ||
                src.includes("logo")
            ) {
                return img.src;
            }
        }

        const meta = document.querySelector("meta[property='og:image']");
        if (meta) return meta.content;

        const icon = document.querySelector("link[rel='icon']");
        if (icon) return icon.href;

        return null;
    }
    """)


# ---------------------------------------------------------
# AUTO SCROLL
# ---------------------------------------------------------

def auto_scroll(page):
    page.evaluate("""
        async () => {
            const delay = ms => new Promise(res => setTimeout(res, ms));

            let totalHeight = 0;
            const distance = 700;
            let previousHeight = -1;
            let stableRounds = 0;

            while (stableRounds < 2) {
                const currentHeight = document.body.scrollHeight;

                if (currentHeight === previousHeight) {
                    stableRounds += 1;
                } else {
                    stableRounds = 0;
                    previousHeight = currentHeight;
                }

                while (totalHeight < currentHeight) {
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    await delay(250);
                }

                await delay(350);
            }

            window.scrollTo(0, 0);
        }
    """)


# ---------------------------------------------------------
# LAZY LOAD TRIGGER
# ---------------------------------------------------------

def trigger_lazy_loading(page):
    page.evaluate("""
        () => {
            const events = ['mouseover', 'mouseenter', 'mousemove'];

            document.querySelectorAll("*").forEach(el => {
                events.forEach(eventType => {
                    el.dispatchEvent(new MouseEvent(eventType, { bubbles: true }));
                });
            });
        }
    """)


# ---------------------------------------------------------
# IMAGE EXTRACTION
# ---------------------------------------------------------

def extract_images(page):
    return page.evaluate("""
    () => {
        const results = new Set();

        document.querySelectorAll("img").forEach(img => {
            if (img.src) results.add(img.src);

            if (img.srcset) {
                img.srcset.split(",").forEach(s => {
                    const url = s.trim().split(" ")[0];
                    if (url) results.add(url);
                });
            }

            if (img.dataset) {
                Object.values(img.dataset).forEach(v => {
                    if (typeof v === "string" && v.startsWith("http")) {
                        results.add(v);
                    }
                });
            }
        });

        document.querySelectorAll("source").forEach(src => {
            if (src.src) results.add(src.src);

            if (src.srcset) {
                src.srcset.split(",").forEach(s => {
                    const url = s.trim().split(" ")[0];
                    if (url) results.add(url);
                });
            }
        });

        document.querySelectorAll("*").forEach(el => {
            const style = window.getComputedStyle(el);

            const props = [
                style.backgroundImage,
                style.background,
                style.content
            ];

            props.forEach(bg => {
                if (bg && bg.includes("url(")) {
                    const matches = bg.match(/url\\((.*?)\\)/g);

                    if (matches) {
                        matches.forEach(m => {
                            let url = m.replace("url(", "")
                                       .replace(")", "")
                                       .replace(/["']/g, "");

                            if (url && url.startsWith("http")) {
                                results.add(url);
                            }
                        });
                    }
                }
            });
        });

        return Array.from(results);
    }
    """)


# ---------------------------------------------------------
# INTERNAL LINK EXTRACTOR
# ---------------------------------------------------------

def extract_internal_links(soup, base_url):
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if href.startswith("#"):
            continue

        full_url = urljoin(base_url, href)

        if full_url.startswith(base_url):
            links.add(full_url)

    return list(links)


# ---------------------------------------------------------
# LINK HELPERS
# ---------------------------------------------------------

HARD_SKIP_KEYWORDS = [
    "login", "log-in", "signin", "sign-in", "signup", "sign-up",
    "account", "youraccount", "my-account",
    "redeem",
    "watch",
    "checkout", "cart",
    "privacy", "terms", "policy", "policies",
    "help", "support", "faq",
    "contact-us-form",
    "careers", "jobs",
    "track", "tracking",
    "password",
    "admin",
    "auth",
    "forgot-password",
    "reset-password"
]

POSITIVE_KEYWORDS = [
    "menu", "menus",
    "product", "products",
    "shop", "store", "stores",
    "services", "service",
    "collections", "collection",
    "coffee", "drinks",
    "about", "about-us",
    "locations", "location",
    "offers", "offer", "special",
    "featured", "feature",
    "plans", "pricing",
    "catalog", "catalogue",
    "brands", "brand",
    "explore",
    "restaurant",
    "order",
    "flavors", "flavour",
    "items", "gallery",
    "portfolio",
    "solutions"
]


def should_hard_skip_link(link):
    lower = link.lower()
    return any(k in lower for k in HARD_SKIP_KEYWORDS)


# ---------------------------------------------------------
# CLEAN / FILTER INTERNAL LINKS
# ---------------------------------------------------------

def clean_internal_links(links, base_url):
    cleaned = []
    seen = set()

    blocked_patterns = [
        "#",
        "mailto:",
        "tel:",
        "javascript:",
        ".pdf",
        ".jpg",
        ".jpeg",
        ".png",
        ".webp",
        ".svg",
        ".gif",
        ".mp4",
        ".mp3",
        ".zip"
    ]

    for link in links:
        if not link:
            continue

        normalized = link.strip().rstrip("/")

        if normalized == base_url.rstrip("/"):
            continue

        lower = normalized.lower()

        if any(x in lower for x in blocked_patterns):
            continue

        if should_hard_skip_link(lower):
            continue

        if lower in seen:
            continue

        seen.add(lower)
        cleaned.append(normalized)

    return cleaned


# ---------------------------------------------------------
# PRIORITIZE IMPORTANT PAGES
# ---------------------------------------------------------

def prioritize_links(links):
    scored = []

    for link in links:
        lower = link.lower()
        score = 0

        for k in POSITIVE_KEYWORDS:
            if k in lower:
                score += 5

        path_parts = [p for p in urlparse(link).path.split("/") if p]
        score -= max(0, len(path_parts) - 2)

        scored.append((score, link))

    scored.sort(key=lambda x: x[0], reverse=True)

    strong_links = [link for score, link in scored if score > 0]
    medium_links = [link for score, link in scored if score == 0]

    if strong_links:
        return strong_links[:MAX_INTERNAL_PAGES_FAST]

    return medium_links[:1]


# ---------------------------------------------------------
# WAIT FOR IMAGE SETTLE
# ---------------------------------------------------------

def settle_after_lazy(page):
    page.wait_for_timeout(1500)


# ---------------------------------------------------------
# SCRAPE CURRENTLY LOADED PAGE
# ---------------------------------------------------------

def collect_images_from_loaded_page(page, current_url, network_images):
    auto_scroll(page)
    trigger_lazy_loading(page)
    settle_after_lazy(page)

    dom_images = extract_images(page)

    print(f"[PAGE] {current_url}")
    print(f"[PAGE] DOM: {len(dom_images)} | Network: {len(network_images)}")

    return list(set(dom_images + list(network_images)))


# ---------------------------------------------------------
# SCRAPE SINGLE PAGE
# ---------------------------------------------------------

def scrape_page(page, url):
    page_network_images = set()

    def handle_response(response):
        try:
            u = response.url.lower()
            if any(ext in u for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                page_network_images.add(response.url)
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

        return collect_images_from_loaded_page(page, url, page_network_images)

    finally:
        try:
            page.remove_listener("response", handle_response)
        except Exception:
            pass


# ---------------------------------------------------------
# STAGE 1: SCRAPE BUSINESS CORE DATA
# ---------------------------------------------------------

def scrape_business_core(url: str, task_id: int):
    print("[SCRAPER] Starting business core scrape")

    try:
        url = normalize_input_url(url)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            parsed = urlparse(page.url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"

            business_name = soup.title.text.strip() if soup.title else None
            business_info = extract_business_text(soup)

            logo_url = detect_logo(page, soup)
            logo_path = download_logo(logo_url, task_id) if logo_url else None

            internal_links = extract_internal_links(soup, base_url)
            internal_links = clean_internal_links(internal_links, base_url)
            top_links = prioritize_links(internal_links)

            browser.close()

            return {
                "business_name": business_name,
                "business_info": business_info,
                "logo_url": logo_path,
                "base_url": base_url,
                "top_links": top_links
            }

    except Exception as e:
        print("Business core scrape failed:", e)
        return None


# ---------------------------------------------------------
# STAGE 2: SCRAPE IMAGE ASSETS
# ---------------------------------------------------------

def scrape_image_assets(url: str, task_id: int, top_links=None):
    print("[SCRAPER] Starting image asset scrape")

    try:
        url = normalize_input_url(url)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            homepage_network_images = set()

            def handle_homepage_response(response):
                try:
                    u = response.url.lower()
                    if any(ext in u for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                        homepage_network_images.add(response.url)
                except Exception:
                    pass

            page.on("response", handle_homepage_response)

            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            homepage_images = collect_images_from_loaded_page(
                page,
                url,
                homepage_network_images
            )

            try:
                page.remove_listener("response", handle_homepage_response)
            except Exception:
                pass

            parsed = urlparse(page.url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"

            all_images = set(homepage_images)

            if len(all_images) < HOMEPAGE_IMAGE_TARGET:
                links_to_use = top_links or []
            else:
                links_to_use = []

            print(f"[SCRAPER] Top internal pages: {len(links_to_use)}")

            for link in links_to_use:
                try:
                    imgs = scrape_page(page, link)
                    all_images.update(imgs)
                except Exception as page_error:
                    print(f"[SCRAPER] Failed page: {link} | {page_error}")
                    continue

            browser.close()

            print(f"[SCRAPER] Total collected images: {len(all_images)}")

            normalized_images = normalize_urls(list(all_images), base_url)
            images = download_images(normalized_images, task_id)

            print(f"[SCRAPER] Final stored images: {len(images)}")

            return images

    except Exception as e:
        print("Image asset scrape failed:", e)
        return []


# ---------------------------------------------------------
# MAIN SCRAPER (BACKWARD COMPATIBLE)
# ---------------------------------------------------------

def scrape_website(url: str, task_id: int):
    print("Starting scrape")

    try:
        url = normalize_input_url(url)

        core_result = scrape_business_core(url, task_id)

        if not core_result:
            return None

        images = scrape_image_assets(
            url=url,
            task_id=task_id,
            top_links=core_result.get("top_links", [])
        )

        return {
            "business_name": core_result.get("business_name"),
            "business_info": core_result.get("business_info"),
            "logo_url": core_result.get("logo_url"),
            "images": images
        }

    except Exception as e:
        print("Scraper failed:", e)
        return None


# ---------------------------------------------------------
# NORMALIZE
# ---------------------------------------------------------

def normalize_urls(images, base_url):
    normalized = []

    for img in images:
        if not img:
            continue

        img = img.strip()

        if img.startswith("//"):
            img = "https:" + img
        elif img.startswith("/"):
            img = base_url + img
        elif not img.startswith("http"):
            img = base_url + "/" + img

        normalized.append(img)

    return list(dict.fromkeys(normalized))


def normalize_url_for_matching(url: str) -> str:
    if not url:
        return ""

    cleaned = str(url).strip().lower()

    if not cleaned:
        return ""

    if not cleaned.startswith(("http://", "https://")):
        cleaned = "https://" + cleaned

    parsed = urlparse(cleaned)

    host = (parsed.netloc or "").strip().lower()

    if not host:
        return ""

    if host.startswith("www."):
        host = host[4:]

    return host