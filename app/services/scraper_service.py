import json
import re
import time
from urllib.parse import urlparse, urljoin, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from app.services.storage_service import download_images, download_logo


# ---------------------------------------------------------
# FAST / HYBRID SETTINGS
# ---------------------------------------------------------

HOMEPAGE_IMAGE_TARGET = 14
MAX_INTERNAL_PAGES_FAST = 3
PAGE_TIMEOUT_MS = 45000

FAST_SETTLE_MS = 650
HEAVY_SETTLE_MS = 1100

HOMEPAGE_EXTRA_WAIT_MS = 850
INTERNAL_EXTRA_WAIT_MS = 450

MAX_IMAGE_URLS_TO_DOWNLOAD = 38
SCRAPE_SESSION_CACHE_TTL_SECONDS = 300

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

BLOCK_PAGE_KEYWORDS = [
    "cloudflare",
    "attention required",
    "checking your browser",
    "verify you are human",
    "security check",
    "access denied",
    "just a moment"
]

# short-lived in-memory cache to avoid reloading homepage twice
# in the same task flow when router calls business_core + image_assets separately
_SCRAPE_SESSION_CACHE = {}


# ---------------------------------------------------------
# CACHE HELPERS
# ---------------------------------------------------------

def _cache_key(url: str) -> str:
    return normalize_url_for_matching(url)


def _get_cached_session(url: str):
    key = _cache_key(url)
    item = _SCRAPE_SESSION_CACHE.get(key)

    if not item:
        return None

    if item["expires_at"] < time.time():
        _SCRAPE_SESSION_CACHE.pop(key, None)
        return None

    return item["data"]


def _set_cached_session(url: str, data: dict):
    key = _cache_key(url)
    _SCRAPE_SESSION_CACHE[key] = {
        "expires_at": time.time() + SCRAPE_SESSION_CACHE_TTL_SECONDS,
        "data": data
    }


# ---------------------------------------------------------
# URL NORMALIZATION
# ---------------------------------------------------------

def normalize_input_url(url: str) -> str:
    if not url:
        raise ValueError("URL is required")

    cleaned = str(url).strip()

    if not cleaned:
        raise ValueError("URL is required")

    if not cleaned.startswith(("http://", "https://")):
        cleaned = "https://" + cleaned

    parsed = urlparse(cleaned)

    if not parsed.netloc:
        raise ValueError(f"Invalid URL: {url}")

    return cleaned


def normalize_url_for_matching(url: str) -> str:
    """
    Host-level normalization for cache/reuse matching.

    Examples:
    - https://www.netflix.com
    - http://netflix.com
    -> netflix.com
    """
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


def canonicalize_url(url: str) -> str:
    if not url:
        return ""

    try:
        parsed = urlparse(url.strip())
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc.lower()

        if netloc.startswith("www."):
            netloc = netloc[4:]

        path = parsed.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        return urlunparse((scheme, netloc, path, "", "", ""))
    except Exception:
        return url


# ---------------------------------------------------------
# PAGE VALIDATION / BLOCK DETECTION
# ---------------------------------------------------------

def is_block_or_challenge_page(page, soup) -> bool:
    try:
        title = (soup.title.text or "").strip().lower() if soup.title else ""
    except Exception:
        title = ""

    text = ""
    try:
        text = soup.get_text(" ", strip=True).lower()[:5000]
    except Exception:
        pass

    combined = f"{title} {text}"

    if any(k in combined for k in BLOCK_PAGE_KEYWORDS):
        return True

    try:
        body_text = page.evaluate("""
        () => (document.body?.innerText || "").slice(0, 4000).toLowerCase()
        """)
        if any(k in body_text for k in BLOCK_PAGE_KEYWORDS):
            return True
    except Exception:
        pass

    return False


def page_looks_weak(soup, dom_images_count: int, network_images_count: int) -> bool:
    text_len = 0
    try:
        text_len = len(soup.get_text(" ", strip=True))
    except Exception:
        pass

    if dom_images_count <= 2 and network_images_count <= 2:
        return True

    if text_len < 400:
        return True

    title = ""
    try:
        title = (soup.title.text or "").strip().lower() if soup.title else ""
    except Exception:
        pass

    if any(k in title for k in BLOCK_PAGE_KEYWORDS):
        return True

    return False


# ---------------------------------------------------------
# BUSINESS TEXT EXTRACTION
# ---------------------------------------------------------

def extract_business_text(soup):
    texts = []

    for tag in soup.find_all(["h1", "h2", "h3", "p"]):
        text = tag.get_text(" ", strip=True)

        if len(text) < 40:
            continue

        lower = text.lower()

        if any(x in lower for x in [
            "cookie", "privacy", "login", "subscribe", "terms",
            "javascript", "browser", "captcha", "security"
        ]):
            continue

        texts.append(text)

    return " ".join(texts[:12])


# ---------------------------------------------------------
# STRING CLEANERS
# ---------------------------------------------------------

def clean_business_name(name: str) -> str:
    if not name:
        return ""

    cleaned = re.sub(r"\s+", " ", str(name)).strip()
    cleaned = re.sub(r"\s*[\|\-–—].*$", "", cleaned).strip()
    cleaned = re.sub(r"\s+(home|official site|homepage)$", "", cleaned, flags=re.I).strip()

    junk = {
        "cloudflare",
        "attention required",
        "access denied",
        "just a moment",
        "security check"
    }

    if cleaned.lower() in junk:
        return ""

    return cleaned


def title_case_domain_name(host: str) -> str:
    host = (host or "").lower()
    if host.startswith("www."):
        host = host[4:]

    core = host.split(".")[0]
    core = re.sub(r"[-_]+", " ", core).strip()

    if not core:
        return ""

    return " ".join(part.capitalize() for part in core.split())


# ---------------------------------------------------------
# BUSINESS NAME EXTRACTION
# ---------------------------------------------------------

def extract_json_ld_names(soup):
    candidates = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        def walk(node):
            if isinstance(node, dict):
                node_type = node.get("@type")
                name = node.get("name")

                if name and (
                    node_type in [
                        "Organization",
                        "AutoDealer",
                        "LocalBusiness",
                        "Corporation",
                        "Store",
                        "AutoRepair"
                    ] or "dealer" in str(node_type).lower()
                    or "business" in str(node_type).lower()
                    or "organization" in str(node_type).lower()
                ):
                    candidates.append(str(name).strip())

                for value in node.values():
                    walk(value)

            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(data)

    return candidates


def extract_business_name(page, soup, current_url):
    candidates = []

    meta_og_site = soup.find("meta", attrs={"property": "og:site_name"})
    if meta_og_site and meta_og_site.get("content"):
        candidates.append(meta_og_site["content"].strip())

    meta_app = soup.find("meta", attrs={"name": "application-name"})
    if meta_app and meta_app.get("content"):
        candidates.append(meta_app["content"].strip())

    candidates.extend(extract_json_ld_names(soup))

    try:
        dom_name = page.evaluate("""
        () => {
            const picks = [];

            const selectors = [
                "header img[alt]",
                "a[aria-label]",
                "[class*='logo'] img[alt]",
                "[class*='brand'] img[alt]",
                "header [class*='logo']",
                "header [class*='brand']",
                ".navbar-brand",
                ".logo",
                ".brand"
            ];

            for (const sel of selectors) {
                document.querySelectorAll(sel).forEach(el => {
                    const values = [
                        el.getAttribute("alt"),
                        el.getAttribute("aria-label"),
                        el.textContent
                    ];

                    values.forEach(v => {
                        if (v && v.trim().length > 1 && v.trim().length < 120) {
                            picks.push(v.trim());
                        }
                    });
                });
            }

            return picks;
        }
        """)
        if dom_name:
            candidates.extend(dom_name)
    except Exception:
        pass

    if soup.title and soup.title.text:
        candidates.append(soup.title.text.strip())

    normalized = []
    seen = set()

    for c in candidates:
        cleaned = clean_business_name(c)
        if not cleaned:
            continue

        low = cleaned.lower()
        if low in seen:
            continue

        seen.add(low)
        normalized.append(cleaned)

    if normalized:
        def score_name(name):
            score = 0
            low = name.lower()

            if 2 <= len(name) <= 60:
                score += 4
            if "cadillac" in low:
                score += 3
            if "motors" in low or "dealer" in low or "auto" in low:
                score += 2
            if re.search(r"[|]", name):
                score -= 3
            if any(bad in low for bad in BLOCK_PAGE_KEYWORDS):
                score -= 100
            return score

        normalized.sort(key=score_name, reverse=True)
        return normalized[0]

    return title_case_domain_name(urlparse(current_url).netloc)


# ---------------------------------------------------------
# LOGO DETECTION
# ---------------------------------------------------------

def detect_logo(page, soup):
    try:
        logo = page.evaluate("""
        () => {
            const candidates = [];

            const pushIfValid = (value) => {
                if (value && typeof value === "string" && value.trim()) {
                    candidates.push(value.trim());
                }
            };

            document.querySelectorAll("img").forEach(img => {
                const alt = (img.getAttribute("alt") || "").toLowerCase();
                const cls = (img.getAttribute("class") || "").toLowerCase();
                const src = img.getAttribute("src") || "";
                const current = img.currentSrc || img.src || "";

                if (
                    alt.includes("logo") ||
                    cls.includes("logo") ||
                    cls.includes("brand") ||
                    src.toLowerCase().includes("logo") ||
                    current.toLowerCase().includes("logo")
                ) {
                    pushIfValid(current || src);
                }
            });

            const logoLike = document.querySelector(
                "header img, .logo img, .navbar-brand img, [class*='brand'] img"
            );
            if (logoLike) {
                pushIfValid(logoLike.currentSrc || logoLike.src || logoLike.getAttribute("src"));
            }

            return candidates;
        }
        """)
        if logo:
            for item in logo:
                if item and item.startswith(("http://", "https://", "//", "/")):
                    return item
    except Exception:
        pass

    meta = soup.find("meta", attrs={"property": "og:image"})
    if meta and meta.get("content"):
        return meta["content"]

    icon = soup.find("link", attrs={"rel": lambda x: x and "icon" in str(x).lower()})
    if icon and icon.get("href"):
        return icon["href"]

    return None


# ---------------------------------------------------------
# BROWSER / PAGE SETUP
# ---------------------------------------------------------

def create_browser_page(playwright):
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage"
        ]
    )

    context = browser.new_context(
        viewport={"width": 1365, "height": 1800},
        user_agent=BROWSER_USER_AGENT,
        locale="en-US",
        timezone_id="America/Chicago"
    )

    page = context.new_page()

    page.add_init_script("""
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    """)

    return browser, context, page


# ---------------------------------------------------------
# AUTO SCROLL / LAZY LOAD
# ---------------------------------------------------------

def auto_scroll(page, max_rounds=1, delay_ms=130, step=950):
    page.evaluate(f"""
        async () => {{
            const delay = ms => new Promise(res => setTimeout(res, ms));

            let rounds = 0;
            while (rounds < {max_rounds}) {{
                const height = document.body.scrollHeight || 0;
                let pos = 0;

                while (pos < height) {{
                    window.scrollTo(0, pos);
                    pos += {step};
                    await delay({delay_ms});
                }}

                rounds += 1;
                await delay(150);
            }}

            window.scrollTo(0, 0);
        }}
    """)


def trigger_lazy_loading(page):
    page.evaluate("""
        () => {
            const events = ["mouseover", "mouseenter"];

            document.querySelectorAll("img, picture, source, section, div, a, button").forEach(el => {
                events.forEach(eventType => {
                    try {
                        el.dispatchEvent(new MouseEvent(eventType, { bubbles: true }));
                    } catch (e) {}
                });
            });
        }
    """)


def prepare_homepage_fast(page, url):
    page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=2500)
    except Exception:
        pass
    page.wait_for_timeout(HOMEPAGE_EXTRA_WAIT_MS)
    auto_scroll(page, max_rounds=1, delay_ms=130, step=950)
    trigger_lazy_loading(page)
    page.wait_for_timeout(FAST_SETTLE_MS)


def prepare_homepage_heavy(page, url):
    page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=5500)
    except Exception:
        pass
    page.wait_for_timeout(1300)
    auto_scroll(page, max_rounds=2, delay_ms=160, step=850)
    trigger_lazy_loading(page)
    page.wait_for_timeout(HEAVY_SETTLE_MS)


def prepare_internal_page(page, url, heavy=False):
    page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
    page.wait_for_timeout(INTERNAL_EXTRA_WAIT_MS if not heavy else 750)
    auto_scroll(page, max_rounds=1 if not heavy else 2, delay_ms=130 if not heavy else 160, step=950 if not heavy else 850)
    trigger_lazy_loading(page)
    page.wait_for_timeout(FAST_SETTLE_MS if not heavy else HEAVY_SETTLE_MS)


# ---------------------------------------------------------
# IMAGE EXTRACTION
# ---------------------------------------------------------

def extract_images(page):
    return page.evaluate("""
    () => {
        const results = new Set();

        const addUrl = (value) => {
            if (!value || typeof value !== "string") return;

            let v = value.trim();
            if (!v) return;
            if (v.startsWith("data:")) return;

            if (v.startsWith("//")) {
                v = "https:" + v;
            }

            results.add(v);
        };

        const extractSrcset = (srcset) => {
            if (!srcset || typeof srcset !== "string") return;
            srcset.split(",").forEach(item => {
                const url = item.trim().split(" ")[0];
                if (url) addUrl(url);
            });
        };

        document.querySelectorAll("img").forEach(img => {
            [
                img.currentSrc,
                img.src,
                img.getAttribute("src"),
                img.getAttribute("data-src"),
                img.getAttribute("data-lazy-src"),
                img.getAttribute("data-original"),
                img.getAttribute("data-image"),
                img.getAttribute("data-bg"),
                img.getAttribute("data-background-image"),
                img.getAttribute("data-srcset"),
                img.getAttribute("data-lazy"),
                img.getAttribute("data-zoom-image")
            ].forEach(addUrl);

            extractSrcset(img.srcset);
            extractSrcset(img.getAttribute("srcset"));
            extractSrcset(img.getAttribute("data-srcset"));
        });

        document.querySelectorAll("source").forEach(src => {
            addUrl(src.src);
            addUrl(src.getAttribute("src"));
            addUrl(src.getAttribute("data-src"));
            extractSrcset(src.srcset);
            extractSrcset(src.getAttribute("srcset"));
        });

        document.querySelectorAll("[style]").forEach(el => {
            const style = el.getAttribute("style") || "";
            const matches = style.match(/url\\((.*?)\\)/g);
            if (matches) {
                matches.forEach(m => {
                    let url = m.replace("url(", "").replace(")", "").replace(/["']/g, "").trim();
                    addUrl(url);
                });
            }
        });

        document.querySelectorAll("section, div, a").forEach(el => {
            const style = getComputedStyle(el);
            [style.backgroundImage, style.background].forEach(bg => {
                if (bg && bg.includes("url(")) {
                    const matches = bg.match(/url\\((.*?)\\)/g);
                    if (matches) {
                        matches.forEach(m => {
                            let url = m.replace("url(", "").replace(")", "").replace(/["']/g, "").trim();
                            addUrl(url);
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
    base_host = normalize_url_for_matching(base_url)

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()

        if not href or href.startswith("#"):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        if not parsed.scheme.startswith("http"):
            continue

        if normalize_url_for_matching(full_url) == base_host:
            links.add(canonicalize_url(full_url))

    return list(links)


# ---------------------------------------------------------
# LINK HELPERS
# ---------------------------------------------------------

HARD_SKIP_KEYWORDS = [
    "login", "log-in", "signin", "sign-in", "signup", "sign-up",
    "account", "youraccount", "my-account",
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
    "reset-password",
    "captcha",
    "verify",
    "cloudflare"
]

POSITIVE_KEYWORDS = [
    "products", "product", "services", "service", "offers", "offer",
    "specials", "special", "featured", "gallery", "portfolio",
    "about", "locations", "location", "solutions", "explore",
    "plans", "pricing", "shop", "store", "brand", "brands",

    "inventory", "new", "used", "pre-owned", "certified",
    "vehicles", "vehicle", "models", "model",
    "finance", "parts", "lease", "service",
    "cadillac", "escalade", "lyriq", "optiq", "xt4", "xt5", "xt6"
]


def should_hard_skip_link(link):
    lower = link.lower()
    return any(k in lower for k in HARD_SKIP_KEYWORDS)


def clean_internal_links(links, base_url):
    cleaned = []
    seen = set()
    base_canonical = canonicalize_url(base_url)

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

        normalized = canonicalize_url(link)
        lower = normalized.lower()

        if normalized == base_canonical:
            continue

        if any(x in lower for x in blocked_patterns):
            continue

        if should_hard_skip_link(lower):
            continue

        if lower in seen:
            continue

        seen.add(lower)
        cleaned.append(normalized)

    return cleaned


def get_fallback_internal_links(base_url):
    candidates = [
        "/inventory/",
        "/new-vehicles/",
        "/used-vehicles/",
        "/pre-owned-vehicles/",
        "/specials/",
        "/offers/",
        "/service/",
        "/parts/",
        "/finance/",
        "/vehicles/",
        "/new/",
        "/used/",
        "/about/",
        "/pricing/"
    ]

    fallback_links = []
    seen = set()

    for path in candidates:
        full = canonicalize_url(urljoin(base_url, path))
        if full not in seen:
            seen.add(full)
            fallback_links.append(full)

    return fallback_links[:MAX_INTERNAL_PAGES_FAST]


def prioritize_links(links):
    scored = []

    for link in links:
        lower = link.lower()
        score = 0

        for k in POSITIVE_KEYWORDS:
            if k in lower:
                score += 5

        path_parts = [p for p in urlparse(link).path.split("/") if p]
        if len(path_parts) <= 2:
            score += 2
        else:
            score -= max(0, len(path_parts) - 3)

        if "inventory" in lower:
            score += 4
        if "new" in lower or "used" in lower or "pre-owned" in lower:
            score += 3
        if "specials" in lower or "offers" in lower:
            score += 3
        if "service" in lower or "parts" in lower:
            score += 2
        if "pricing" in lower or "plans" in lower:
            score += 2

        scored.append((score, link))

    scored.sort(key=lambda x: x[0], reverse=True)

    strong_links = [link for score, link in scored if score > 0]
    medium_links = [link for score, link in scored if score == 0]

    selected = strong_links[:MAX_INTERNAL_PAGES_FAST]

    if len(selected) < MAX_INTERNAL_PAGES_FAST:
        remaining = MAX_INTERNAL_PAGES_FAST - len(selected)
        selected.extend(medium_links[:remaining])

    return selected


# ---------------------------------------------------------
# PAGE IMAGE COLLECTION
# ---------------------------------------------------------

def collect_images_from_loaded_page(page, current_url, network_images):
    dom_images = extract_images(page)

    print(f"[PAGE] {current_url}")
    print(f"[PAGE] DOM: {len(dom_images)} | Network: {len(network_images)}")

    return list(set(dom_images + list(network_images)))


def filter_network_image_url(url: str) -> bool:
    if not url:
        return False

    lower = url.lower()

    if not any(ext in lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
        return False

    bad = [
        "favicon", "logo", "icon", "sprite",
        "1x1", "pixel", "blank", "placeholder"
    ]

    if any(x in lower for x in bad):
        return False

    return True


# ---------------------------------------------------------
# IMAGE URL RANKING / CAPPING BEFORE STORAGE
# ---------------------------------------------------------

def score_image_url(url: str) -> int:
    lower = (url or "").lower()
    score = 0

    good_terms = [
        "hero", "banner", "vehicle", "inventory", "model",
        "cadillac", "escalade", "lyriq", "optiq", "xt4", "xt5", "xt6",
        "special", "offer"
    ]
    for term in good_terms:
        if term in lower:
            score += 3

    bad_terms = [
        "logo", "icon", "sprite", "placeholder", "favicon",
        "thumb", "thumbnail", "avatar", "badge"
    ]
    for term in bad_terms:
        if term in lower:
            score -= 6

    if any(ext in lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
        score += 1

    return score


def reduce_image_candidates(image_urls, max_urls=MAX_IMAGE_URLS_TO_DOWNLOAD):
    deduped = []
    seen = set()

    for url in image_urls:
        canonical = canonicalize_url(str(url).strip())
        if not canonical:
            continue
        if canonical.lower() in seen:
            continue
        seen.add(canonical.lower())
        deduped.append(url)

    ranked = sorted(deduped, key=score_image_url, reverse=True)
    return ranked[:max_urls]


# ---------------------------------------------------------
# DEBUG HELPERS
# ---------------------------------------------------------

def print_page_debug(page, soup, html):
    try:
        print("[DEBUG] Final page URL:", page.url)
    except Exception:
        pass

    try:
        print("[DEBUG] Page title:", soup.title.text.strip() if soup.title else "NO_TITLE")
    except Exception:
        print("[DEBUG] Page title: NO_TITLE")

    try:
        print("[DEBUG] Body preview:", soup.get_text(" ", strip=True)[:500])
    except Exception:
        print("[DEBUG] Body preview: ")

    try:
        print("[DEBUG] HTML length:", len(html))
    except Exception:
        pass


# ---------------------------------------------------------
# HOMEPAGE LOAD + ESCALATION
# ---------------------------------------------------------

def load_homepage_with_mode(page, url):
    prepare_homepage_fast(page, url)

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    dom_images = extract_images(page)

    weak = page_looks_weak(soup, len(dom_images), 0) or is_block_or_challenge_page(page, soup)

    if weak:
        print("[SCRAPER] Escalating homepage load to heavy mode")
        prepare_homepage_heavy(page, url)
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

    return html, soup


# ---------------------------------------------------------
# CORE + HOMEPAGE IMAGE PREP IN SINGLE PASS
# ---------------------------------------------------------

def scrape_bundle_once(url: str, task_id: int):
    url = normalize_input_url(url)

    with sync_playwright() as p:
        browser, context, page = create_browser_page(p)

        homepage_network_images = set()

        def handle_homepage_response(response):
            try:
                if filter_network_image_url(response.url):
                    homepage_network_images.add(response.url)
            except Exception:
                pass

        page.on("response", handle_homepage_response)

        load_homepage_with_mode(page, url)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        print_page_debug(page, soup, html)

        parsed = urlparse(page.url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        blocked = is_block_or_challenge_page(page, soup)
        print("[DEBUG] blocked_page:", blocked)

        business_name = extract_business_name(page, soup, page.url)
        business_info = extract_business_text(soup)

        logo_url = detect_logo(page, soup)
        logo_url = urljoin(base_url, logo_url) if logo_url else None
        logo_path = download_logo(logo_url, task_id) if logo_url else None

        internal_links = extract_internal_links(soup, base_url)
        internal_links = clean_internal_links(internal_links, base_url)
        top_links = prioritize_links(internal_links)

        if not top_links:
            top_links = get_fallback_internal_links(base_url)

        homepage_images = collect_images_from_loaded_page(
            page,
            page.url,
            homepage_network_images
        )

        print("[DEBUG] Business name:", business_name)
        print("[DEBUG] Internal links found:", len(internal_links))
        print("[DEBUG] Top links selected:", len(top_links))
        print("[DEBUG] Homepage image candidates before normalize:", len(homepage_images))

        try:
            page.remove_listener("response", handle_homepage_response)
        except Exception:
            pass

        context.close()
        browser.close()

        bundle = {
            "business_name": business_name,
            "business_info": business_info,
            "logo_url": logo_path,
            "base_url": base_url,
            "top_links": top_links,
            "blocked_page": blocked,
            "homepage_images": homepage_images
        }

        _set_cached_session(url, bundle)
        return bundle


# ---------------------------------------------------------
# SCRAPE SINGLE PAGE
# ---------------------------------------------------------

def scrape_page(page, url):
    page_network_images = set()

    def handle_response(response):
        try:
            u = response.url
            if filter_network_image_url(u):
                page_network_images.add(u)
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        prepare_internal_page(page, url, heavy=False)

        dom_images = extract_images(page)
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        if page_looks_weak(soup, len(dom_images), len(page_network_images)):
            print(f"[SCRAPER] Escalating internal page to heavy mode: {url}")
            prepare_internal_page(page, url, heavy=True)

        return collect_images_from_loaded_page(page, url, page_network_images)

    finally:
        try:
            page.remove_listener("response", handle_response)
        except Exception:
            pass


# ---------------------------------------------------------
# BACKWARD-COMPATIBLE WRAPPERS
# ---------------------------------------------------------

def scrape_business_core(url: str, task_id: int):
    print("[SCRAPER] Starting business core scrape")

    try:
        cached = _get_cached_session(url)
        if cached:
            return {
                "business_name": cached.get("business_name"),
                "business_info": cached.get("business_info"),
                "logo_url": cached.get("logo_url"),
                "base_url": cached.get("base_url"),
                "top_links": cached.get("top_links"),
                "blocked_page": cached.get("blocked_page", False)
            }

        bundle = scrape_bundle_once(url, task_id)

        return {
            "business_name": bundle.get("business_name"),
            "business_info": bundle.get("business_info"),
            "logo_url": bundle.get("logo_url"),
            "base_url": bundle.get("base_url"),
            "top_links": bundle.get("top_links"),
            "blocked_page": bundle.get("blocked_page", False)
        }

    except Exception as e:
        print("Business core scrape failed:", e)
        return None


def scrape_image_assets(url: str, task_id: int, top_links=None):
    print("[SCRAPER] Starting image asset scrape")

    try:
        cached = _get_cached_session(url)

        if not cached:
            cached = scrape_bundle_once(url, task_id)

        all_images = set(cached.get("homepage_images", []))
        base_url = cached.get("base_url")
        links_to_use = (top_links or cached.get("top_links") or [])[:MAX_INTERNAL_PAGES_FAST]

        if not links_to_use:
            links_to_use = get_fallback_internal_links(base_url)

        if len(all_images) >= HOMEPAGE_IMAGE_TARGET:
            links_to_use = links_to_use[:2]

        print(f"[SCRAPER] Top internal pages: {len(links_to_use)}")

        with sync_playwright() as p:
            browser, context, page = create_browser_page(p)

            for link in links_to_use:
                try:
                    imgs = scrape_page(page, link)
                    all_images.update(imgs)
                except Exception as page_error:
                    print(f"[SCRAPER] Failed page: {link} | {page_error}")
                    continue

            context.close()
            browser.close()

        print(f"[SCRAPER] Total collected images: {len(all_images)}")

        normalized_images = normalize_urls(list(all_images), base_url)
        reduced_images = reduce_image_candidates(normalized_images, MAX_IMAGE_URLS_TO_DOWNLOAD)

        print(f"[SCRAPER] Reduced image URLs before storage: {len(reduced_images)}")

        images = download_images(reduced_images, task_id)

        print(f"[SCRAPER] Final stored images: {len(images)}")

        return images

    except Exception as e:
        print("Image asset scrape failed:", e)
        return []


# ---------------------------------------------------------
# MAIN SCRAPER
# ---------------------------------------------------------

def scrape_website(url: str, task_id: int):
    print("Starting scrape")

    try:
        url = normalize_input_url(url)

        print("[SCRAPER] Starting business core scrape")
        bundle = scrape_bundle_once(url, task_id)

        print("[SCRAPER] Starting image asset scrape")

        all_images = set(bundle.get("homepage_images", []))
        base_url = bundle.get("base_url")
        links_to_use = (bundle.get("top_links") or [])[:MAX_INTERNAL_PAGES_FAST]

        if not links_to_use:
            links_to_use = get_fallback_internal_links(base_url)

        if len(all_images) >= HOMEPAGE_IMAGE_TARGET:
            links_to_use = links_to_use[:2]

        print(f"[SCRAPER] Top internal pages: {len(links_to_use)}")

        with sync_playwright() as p:
            browser, context, page = create_browser_page(p)

            for link in links_to_use:
                try:
                    imgs = scrape_page(page, link)
                    all_images.update(imgs)
                except Exception as page_error:
                    print(f"[SCRAPER] Failed page: {link} | {page_error}")
                    continue

            context.close()
            browser.close()

        print(f"[SCRAPER] Total collected images: {len(all_images)}")

        normalized_images = normalize_urls(list(all_images), base_url)
        reduced_images = reduce_image_candidates(normalized_images, MAX_IMAGE_URLS_TO_DOWNLOAD)

        print(f"[SCRAPER] Reduced image URLs before storage: {len(reduced_images)}")

        images = download_images(reduced_images, task_id)

        print(f"[SCRAPER] Final stored images: {len(images)}")

        return {
            "business_name": bundle.get("business_name"),
            "business_info": bundle.get("business_info"),
            "logo_url": bundle.get("logo_url"),
            "images": images,
            "blocked_page": bundle.get("blocked_page", False)
        }

    except Exception as e:
        print("Scraper failed:", e)
        return None


# ---------------------------------------------------------
# NORMALIZE IMAGE URLS
# ---------------------------------------------------------

def normalize_urls(images, base_url):
    normalized = []
    seen = set()

    for img in images:
        if not img:
            continue

        img = str(img).strip()
        if not img:
            continue

        if img.startswith("data:"):
            continue

        if img.startswith("//"):
            img = "https:" + img
        elif img.startswith("/"):
            img = urljoin(base_url, img)
        elif not img.startswith("http"):
            img = urljoin(base_url + "/", img)

        canonical = canonicalize_url(img)

        if canonical.lower() in seen:
            continue

        seen.add(canonical.lower())
        normalized.append(img)

    return normalized