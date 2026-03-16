from playwright.sync_api import sync_playwright
import json
import re
import argparse
import time
import warnings
import os
from pymongo import MongoClient

try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except Exception:
    pass


DEFAULT_URL = "https://collegedunia.com/university/25948-iit-kanpur-indian-institute-of-technology-iitk-kanpur/courses-fees"
DEFAULT_OUTPUT_FILE = "basic_college_courses.json"
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://alishakhan8488_db_user:DaVHn9goL8STNzNs@cluster0.nkmbpqt.mongodb.net/studentcap?retryWrites=true&w=majority",
)
MONGO_DB = os.getenv("MONGO_DB", "studentcap")
MONGO_COLLECTION = os.getenv(
    "SCRAPER_BASIC_COLLEGE_COURSE_MONGO_COLLECTION",
    "college_course",
)
BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
]


def _default_headless():
    return os.getenv("SCRAPER_DEFAULT_HEADLESS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    } or os.getenv("RENDER", "").strip().lower() == "true"


def _should_use_low_memory_mode():
    flag = os.getenv("SCRAPER_LOW_MEMORY_MODE", "").strip().lower()
    if flag in {"1", "true", "yes", "on"}:
        return True
    if flag in {"0", "false", "no", "off"}:
        return False
    return os.getenv("RENDER", "").strip().lower() == "true"


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Scrape Collegedunia courses with optional deep detail extraction."
    )
    parser.add_argument(
        "--url",
        default="",
        help="Override the default Collegedunia courses-fees URL.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Write scraped JSON to this path instead of the default output file.",
    )
    parser.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        help="Run browser in headless mode. Default is headed so progress is visible.",
    )
    parser.add_argument(
        "--headed",
        dest="headless",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--slow-mo",
        type=int,
        default=0,
        help="Slow down browser actions by N milliseconds. Helpful for visual debugging.",
    )
    parser.add_argument(
        "--limit-courses",
        type=int,
        default=None,
        help="Only scrape first N course cards.",
    )
    parser.add_argument(
        "--limit-sub-courses",
        type=int,
        default=None,
        help="Only scrape first N sub-courses per course.",
    )
    parser.add_argument(
        "--skip-course-detail",
        action="store_false",
        dest="fetch_course_detail",
        help="Skip deep TOC scraping for main courses.",
    )
    parser.add_argument(
        "--fetch-course-detail",
        action="store_true",
        dest="fetch_course_detail",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--skip-sub-course-detail",
        action="store_false",
        dest="fetch_sub_course_detail",
        help="Skip deep TOC scraping for sub-courses.",
    )
    parser.add_argument(
        "--fetch-sub-course-detail",
        action="store_true",
        dest="fetch_sub_course_detail",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-sections-per-course",
        type=int,
        default=0,
        help="Limit TOC sections per course while fetching details (0 = no limit).",
    )
    parser.set_defaults(
        fetch_course_detail=True,
        fetch_sub_course_detail=True,
        headless=_default_headless(),
    )
    return parser.parse_args()


def _resolve_runtime_url(cli_url=""):
    return (cli_url or os.getenv("SCRAPER_BASIC_COLLEGE_COURSE_URL") or DEFAULT_URL).strip()


def _resolve_output_file(cli_output_file=""):
    return (
        cli_output_file
        or os.getenv("SCRAPER_BASIC_COLLEGE_COURSE_OUTPUT_FILE")
        or DEFAULT_OUTPUT_FILE
    ).strip()


def _launch_browser(playwright, headless, slow_mo):
    launch_kwargs = {"headless": headless, "args": BROWSER_ARGS}
    if isinstance(slow_mo, int) and slow_mo > 0:
        launch_kwargs["slow_mo"] = slow_mo

    # Prefer installed Google Chrome so the user can see the familiar browser.
    try:
        return playwright.chromium.launch(channel="chrome", **launch_kwargs)
    except Exception as e:
        print(f"[launch] Chrome channel unavailable, using bundled Chromium. ({e})", flush=True)
        return playwright.chromium.launch(**launch_kwargs)


def _route_handler(route):
    if route.request.resource_type in {"image", "font", "media"}:
        route.abort()
    else:
        route.continue_()


def _clean(text):
    return " ".join((text or "").split()).strip()


def _safe_text(locator):
    try:
        if locator.count() > 0:
            return _clean(locator.first.inner_text())
    except Exception:
        pass
    return ""


def _safe_click(page, locator):
    try:
        if locator.count() == 0:
            return False
        target = locator.first
        target.scroll_into_view_if_needed()
        try:
            target.click(timeout=2000)
        except Exception:
            page.evaluate("(el) => el.click()", target)
        return True
    except Exception:
        return False


def _extract_reviews_count(text):
    m = re.search(r"(\d+)\s*Reviews?", text, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_sub_course_count(text):
    m = re.search(r"(\d+)\s*Courses?", text, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_college_id(url):
    m = re.search(r"/(?:university|college)/(\d+)", url)
    return int(m.group(1)) if m else None


def _build_slug_url(href):
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("/"):
        href = "https://collegedunia.com" + href
    href = href.split("?")[0].split("#")[0].rstrip("/")
    # Remove trailing numeric ids, e.g. ...-abm-1563 -> ...-abm
    href = re.sub(r"-\d+$", "", href)
    slug = href.rsplit("/", 1)[-1]
    # Remove trailing mode words from slug.
    slug = re.sub(r"-(full|part)-time$", "", slug, flags=re.IGNORECASE)
    slug = re.sub(r"-(online|offline|distance)$", "", slug, flags=re.IGNORECASE)
    return slug


def _absolute_url(href):
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://collegedunia.com" + href
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return "https://collegedunia.com/" + href.lstrip("/")


def _slugify_name(name):
    s = (name or "").lower()
    s = re.sub(r"\[.*?\]", "", s)  # remove bracket tags like [PGPM]
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    s = s.replace("ph-d", "phd")
    return s


def _normalize_slug_with_name(slug, name, parent_course_name=""):
    slug = _clean(slug).strip("/")
    parent_slug = _slugify_name(parent_course_name)
    name_slug = _slugify_name(name)

    # Generic/non-course slugs should never be saved for a sub-course.
    if not slug or slug in {"courses", "course", "courses-fees", "fees"}:
        if parent_slug == "phd" and name_slug and not name_slug.startswith("phd-"):
            return f"phd-{name_slug}"
        return name_slug
    return slug


def open_courses_fees(page, target_url):
    page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(450)


def _scroll_to_course_cards(page):
    # Try to reach the cards section.
    for _ in range(8):
        cards = page.locator("section div[class*='course-card'], div.course-card")
        if cards.count() > 0:
            break
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(300)

    # Load more cards lazily if present.
    stable_rounds = 0
    prev = -1
    for _ in range(14):
        cards = page.locator("section div[class*='course-card'], div.course-card")
        count = cards.count()
        if count <= prev:
            stable_rounds += 1
        else:
            stable_rounds = 0
            prev = count
        if stable_rounds >= 3:
            break
        page.mouse.wheel(0, 2400)
        page.wait_for_timeout(320)


def _extract_card_fields(card):
    item = {
        "course_name": "",
        "course_url": "",
        "slug_url": "",
        "rating": None,
        "reviews": None,
        "sub_course_count": None,
        "duration": "",
        "mode": "",
        "total_fees": "",
        "eligibility": "",
        "application_date": "",
    }

    # Course name
    name_locator = card.locator("div.course-detail a")
    if name_locator.count() == 0:
        name_locator = card.locator("a[data-event-type='course_section']")

    item["course_name"] = _safe_text(name_locator)
    main_href = ""
    try:
        main_href = name_locator.first.get_attribute("href") or ""
    except Exception:
        main_href = ""
    if not main_href:
        try:
            main_href = name_locator.first.get_attribute("data-ga-href") or ""
        except Exception:
            main_href = ""
    if main_href and not main_href.startswith("http") and not main_href.startswith("/"):
        main_href = "/" + main_href
    item["course_url"] = _absolute_url(main_href)
    item["slug_url"] = _build_slug_url(main_href)
    if not item["slug_url"] or item["slug_url"] == "courses-fees":
        item["slug_url"] = _slugify_name(item["course_name"])

    # Rating
    rating_text = _safe_text(card.locator("div.rating-fees span[class*='font-weight-medium']"))
    if rating_text:
        try:
            item["rating"] = float(re.search(r"\d+(\.\d+)?", rating_text).group(0))
        except Exception:
            item["rating"] = None

    # Reviews
    reviews_text = _safe_text(card.locator("a:has-text('Reviews')"))
    item["reviews"] = _extract_reviews_count(reviews_text)

    # Meta separators: courses / years / mode
    metas = card.locator("span[class*='course-separater']")
    meta_values = []
    for i in range(metas.count()):
        t = _clean(metas.nth(i).inner_text())
        if t:
            meta_values.append(t)

    for t in meta_values:
        if item["sub_course_count"] is None:
            v = _extract_sub_course_count(t)
            if v is not None:
                item["sub_course_count"] = v
                continue
        if not item["duration"] and re.search(r"\bYear", t, flags=re.IGNORECASE):
            item["duration"] = t
            continue
        if not item["mode"] and re.search(r"\b(Full Time|Part Time|Distance|Online)\b", t, flags=re.IGNORECASE):
            item["mode"] = t

    # Total Fees
    fees_block = card.locator("div[class*='text-end'] span")
    fees_texts = []
    for i in range(fees_block.count()):
        t = _clean(fees_block.nth(i).inner_text())
        if t:
            fees_texts.append(t)
    # Usually: ["Total Fees:", "â‚¹20.75 Lakhs"]
    if len(fees_texts) >= 2:
        item["total_fees"] = fees_texts[-1]
    elif fees_texts:
        item["total_fees"] = fees_texts[0]

    # Eligibility
    eligibility_value = _safe_text(card.locator("div.eligibility-section div[class*='text-primary-black']"))
    if not eligibility_value:
        eligibility_value = _safe_text(card.locator("div.eligibility-section div.fs-14"))
    item["eligibility"] = eligibility_value

    # Application Date
    app_value = _safe_text(card.locator("div.application-section div[class*='text-primary-green']"))
    if not app_value:
        app_value = _safe_text(card.locator("div.application-section div.fs-14"))
    item["application_date"] = app_value

    return item


def _typed_text(value):
    return {"type": "text", "value": _clean(value)}


def _typed_or_str_value(value):
    if isinstance(value, dict):
        return _clean(value.get("value"))
    if isinstance(value, str):
        return _clean(value)
    return ""


def _is_probable_sub_course_url(url):
    u = _clean(url).lower()
    if not u or "/university/" not in u:
        return False

    blocked = [
        "/placement",
        "/admission",
        "/review",
        "/reviews",
        "/ranking",
        "/faculty",
        "/gallery",
        "/qna",
        "/cutoff",
        "/scholarship",
        "/hostel",
    ]
    return not any(b in u for b in blocked)


def _looks_like_non_course_row(name):
    t = _clean(name).lower()
    if not t:
        return True
    bad_signals = [
        "highest package",
        "median package",
        "average package",
        "students placed",
        "student admitted",
        "placement package",
        "particulars",
    ]
    return any(s in t for s in bad_signals)


def _needs_sub_course_recovery(sub_courses, expected_count=None):
    if not sub_courses:
        return True

    names = []
    bad_name_count = 0

    for sc in sub_courses:
        if not isinstance(sc, dict):
            continue
        n = _typed_or_str_value(sc.get("name"))
        if n:
            names.append(n)
            if _looks_like_non_course_row(n):
                bad_name_count += 1

    if names and bad_name_count >= max(1, len(names) // 2 + 1):
        return True
    if isinstance(expected_count, int) and expected_count > 0 and len(names) < expected_count:
        return True
    return False


def _build_sub_course_item(name_text, sub_url, parent_course_name="", fees_text=""):
    slug_url = _build_slug_url(sub_url)
    slug_url = _normalize_slug_with_name(slug_url, name_text, parent_course_name)
    return {
        "name": _typed_text(name_text) if name_text else None,
        "url": _typed_text(sub_url) if sub_url else None,
        "slug_url": _typed_text(slug_url) if slug_url else None,
        "rating": None,
        "reviews": None,
        "fees": _typed_text(fees_text) if fees_text else None,
        "application_date": None,
        "cutoff": None,
    }


def _recover_sub_courses_from_course_detail(course_detail, parent_course_name="", expected_count=None):
    recovered = []
    seen_urls = set()

    def _push(name_text, sub_url, fees_text=""):
        sub_url = _absolute_url(sub_url)
        if not _is_probable_sub_course_url(sub_url):
            return
        key = sub_url.lower()
        if key in seen_urls:
            return
        seen_urls.add(key)
        recovered.append(_build_sub_course_item(name_text, sub_url, parent_course_name, fees_text=fees_text))

    sections = []
    if isinstance(course_detail, dict):
        sections = course_detail.get("toc_sections", [])

    for sec in sections:
        content = sec.get("content", []) if isinstance(sec, dict) else []
        for item in content:
            if not isinstance(item, dict):
                continue

            t = item.get("type")
            if t == "link":
                label = _clean(item.get("label"))
                href = _clean(item.get("href"))
                if label and href:
                    _push(label, href)
                continue

            if t != "list":
                continue

            values = item.get("value", [])
            if isinstance(values, str):
                values = [values]

            for raw in values:
                line = _clean(raw)
                if not line:
                    continue
                m = re.search(r"\((https?://[^)]+)\)\s*$", line)
                if not m:
                    continue
                sub_url = m.group(1).strip()
                prefix = _clean(line[:m.start()])
                if not prefix:
                    continue

                fee_text = ""
                fee_match = re.search(
                    r"(₹\s*[\d.,]+(?:\s*(?:Lakhs?|Crores?|LPA|K|Cr|L))?)\s*$",
                    prefix,
                    flags=re.IGNORECASE,
                )
                if fee_match:
                    fee_text = _clean(fee_match.group(1))
                    prefix = _clean(prefix[:fee_match.start()])

                if prefix and not _looks_like_non_course_row(prefix):
                    _push(prefix, sub_url, fees_text=fee_text)

    if isinstance(expected_count, int) and expected_count > 0 and len(recovered) > expected_count:
        recovered = recovered[:expected_count]

    return recovered


def _name_key(name):
    return _slugify_name(_clean(name))


def _merge_sub_course_urls_from_recovered(sub_courses, recovered_sub_courses):
    if not sub_courses or not recovered_sub_courses:
        return sub_courses

    by_name = {}
    for r in recovered_sub_courses:
        rk = _name_key(_typed_or_str_value(r.get("name")))
        if rk and rk not in by_name:
            by_name[rk] = r

    merged = []
    for sc in sub_courses:
        if not isinstance(sc, dict):
            merged.append(sc)
            continue

        current_url = _typed_or_str_value(sc.get("url"))
        if _is_probable_sub_course_url(current_url):
            merged.append(sc)
            continue

        nk = _name_key(_typed_or_str_value(sc.get("name")))
        recovered = by_name.get(nk)
        if not recovered:
            merged.append(sc)
            continue

        sc = dict(sc)
        sc["url"] = recovered.get("url")
        sc["slug_url"] = recovered.get("slug_url")
        if not sc.get("fees") and recovered.get("fees"):
            sc["fees"] = recovered.get("fees")
        merged.append(sc)

    return merged


def _score_sub_course_rows(rows, expected_sub_count=None):
    if not rows:
        return -999

    url_count = 0
    rating_count = 0
    for row in rows:
        if _is_probable_sub_course_url(_typed_or_str_value(row.get("url"))):
            url_count += 1
        if _typed_or_str_value(row.get("rating")):
            rating_count += 1

    score = len(rows) * 10 + url_count * 4 + rating_count
    if isinstance(expected_sub_count, int) and expected_sub_count > 0:
        if len(rows) >= expected_sub_count:
            score += 25
        score -= abs(expected_sub_count - len(rows))
    return score


def _sub_course_row_key(row):
    if not isinstance(row, dict):
        return ""

    name_key = _name_key(_typed_or_str_value(row.get("name")))
    if name_key:
        return f"name:{name_key}"

    slug_key = _clean(_typed_or_str_value(row.get("slug_url"))).lower()
    if slug_key:
        return f"slug:{slug_key}"

    url = _typed_or_str_value(row.get("url"))
    if _is_probable_sub_course_url(url):
        return f"url:{url.lower()}"
    if url:
        return f"raw-url:{url.lower()}"

    return ""


def _merge_sub_course_row(existing, incoming):
    if not isinstance(existing, dict):
        return dict(incoming) if isinstance(incoming, dict) else incoming
    if not isinstance(incoming, dict):
        return existing

    merged = dict(existing)
    for field in [
        "name",
        "url",
        "slug_url",
        "rating",
        "reviews",
        "fees",
        "application_date",
        "cutoff",
        "course_detail",
    ]:
        current_value = merged.get(field)
        incoming_value = incoming.get(field)
        if not incoming_value:
            continue

        if field == "url":
            current_url = _typed_or_str_value(current_value)
            incoming_url = _typed_or_str_value(incoming_value)
            if (
                not current_value
                or (
                    not _is_probable_sub_course_url(current_url)
                    and _is_probable_sub_course_url(incoming_url)
                )
            ):
                merged[field] = incoming_value
            continue

        if field == "slug_url":
            current_slug = _clean(_typed_or_str_value(current_value)).lower()
            incoming_slug = _clean(_typed_or_str_value(incoming_value)).lower()
            if (
                not current_value
                or current_slug in {"courses", "course", "courses-fees", "fees"}
                and incoming_slug not in {"", "courses", "course", "courses-fees", "fees"}
            ):
                merged[field] = incoming_value
            continue

        if not current_value:
            merged[field] = incoming_value

    return merged


def _accumulate_sub_course_rows(accumulator, rows):
    added = 0
    for row in rows:
        key = _sub_course_row_key(row)
        if not key:
            fallback_name = _typed_or_str_value(row.get("name")) or f"row-{len(accumulator) + 1}"
            key = f"fallback:{fallback_name.lower()}"

        if key in accumulator:
            accumulator[key] = _merge_sub_course_row(accumulator[key], row)
            continue

        accumulator[key] = dict(row) if isinstance(row, dict) else row
        added += 1

    return added


def _advance_sub_course_table(page, scope):
    try:
        rows = scope.locator("table tbody tr")
        row_count = rows.count()
        if row_count > 0:
            rows.nth(row_count - 1).scroll_into_view_if_needed()
    except Exception:
        pass

    js_scroll = """
    (el) => {
        const seen = new Set();
        const queue = [el];
        const selectors = ['div.course-other-detail', 'table tbody', 'table'];

        for (const selector of selectors) {
            const node = el.querySelector(selector);
            if (node) queue.push(node);
        }

        for (const node of queue) {
            let current = node;
            for (let depth = 0; depth < 5 && current; depth += 1, current = current.parentElement) {
                if (seen.has(current)) continue;
                seen.add(current);
                if (current.scrollHeight > current.clientHeight + 16) {
                    current.scrollTop = Math.min(
                        current.scrollTop + Math.max(current.clientHeight, 500),
                        current.scrollHeight
                    );
                }
            }
        }
    }
    """

    for selector in ["div.course-other-detail", "table", "table tbody"]:
        try:
            locator = scope.locator(selector)
            if locator.count() > 0:
                locator.first.evaluate(js_scroll)
        except Exception:
            continue

    try:
        page.mouse.wheel(0, 900)
    except Exception:
        pass
    page.wait_for_timeout(350)


def _collect_sub_courses_from_scope(
    page,
    scope,
    selectors,
    parent_course_name="",
    expected_sub_count=None,
    require_probable_url=False,
):
    accumulated = {}
    best_rows = []
    best_score = -999
    stable_rounds = 0
    previous_total = -1

    max_rounds = 1
    if isinstance(expected_sub_count, int) and expected_sub_count > 0:
        max_rounds = min(max(expected_sub_count + 2, 4), 14)

    for attempt in range(max_rounds):
        rows_found_this_round = False

        for selector in selectors:
            tables = scope.locator(selector)
            for ti in range(tables.count()):
                table = tables.nth(ti)
                rows = _parse_sub_course_rows_from_table(
                    table,
                    parent_course_name=parent_course_name,
                    require_probable_url=require_probable_url,
                )
                if not rows:
                    continue

                rows_found_this_round = True
                _accumulate_sub_course_rows(accumulated, rows)

                score = _score_sub_course_rows(rows, expected_sub_count=expected_sub_count)
                if score > best_score:
                    best_score = score
                    best_rows = rows

        merged_rows = list(accumulated.values()) if accumulated else []
        current_total = len(merged_rows) if len(merged_rows) >= len(best_rows) else len(best_rows)
        if isinstance(expected_sub_count, int) and expected_sub_count > 0 and current_total >= expected_sub_count:
            break

        if current_total <= previous_total or not rows_found_this_round:
            stable_rounds += 1
        else:
            stable_rounds = 0
            previous_total = current_total

        if attempt == max_rounds - 1 or stable_rounds >= 2:
            break

        _advance_sub_course_table(page, scope)

    merged_rows = list(accumulated.values())
    if len(merged_rows) >= len(best_rows):
        return merged_rows
    return best_rows


def _extract_sub_course_name_from_cell(name_td):
    selectors = [
        "a:not(:has-text('Review')):not(:has-text('Compare')):not(:has-text('Check Details'))",
        "div[class*='text-primary-black']",
        "div.fs-16",
        "p",
    ]
    for sel in selectors:
        txt = _safe_text(name_td.locator(sel))
        if txt and not _looks_like_non_course_row(txt):
            return _strip_noise(txt)

    raw = ""
    try:
        raw = name_td.inner_text() or ""
    except Exception:
        raw = ""

    for line in raw.splitlines():
        t = _clean(line)
        if not t:
            continue
        if re.search(r"\b(reviews?|compare|check details|viewed)\b", t, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"\d+(\.\d+)?", t):
            continue
        if not _looks_like_non_course_row(t):
            return _strip_noise(t)

    return ""


def _extract_best_sub_course_url_from_row(row):
    best_url = ""
    best_score = -999
    anchors = row.locator("a")

    for i in range(anchors.count()):
        a = anchors.nth(i)
        href = ""
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            href = ""
        if not href:
            try:
                href = a.get_attribute("data-ga-href") or ""
            except Exception:
                href = ""

        raw_url = _absolute_url(href)
        url = raw_url
        review_m = re.search(r"(https?://[^/]+/university/[^/?#]+)/reviews\?course=(\d+)", raw_url, flags=re.IGNORECASE)
        if review_m:
            url = f"{review_m.group(1)}/courses-fees?course_id={review_m.group(2)}"
        if not _is_probable_sub_course_url(url):
            continue

        lurl = url.lower()
        score = 0
        if "/courses-fees?course_id=" in lurl:
            score += 5
        if "post-graduate" in lurl or "programme" in lurl or "phd" in lurl:
            score += 2
        if "/courses-fees" in lurl:
            score += 1
        if review_m:
            score += 3

        if score > best_score:
            best_score = score
            best_url = url

    return best_url


def _parse_sub_course_rows_from_table(table, parent_course_name="", require_probable_url=False):
    parsed = []
    rows = table.locator("tr")

    for i in range(rows.count()):
        row = rows.nth(i)
        tds = row.locator("td")
        if tds.count() < 3:
            continue

        name_td = tds.nth(0)
        fees_td = tds.nth(1)
        app_td = tds.nth(2)
        cutoff_td = tds.nth(3) if tds.count() > 3 else None

        name_text = _extract_sub_course_name_from_cell(name_td)
        if not name_text or _looks_like_non_course_row(name_text):
            continue

        sub_url = _extract_best_sub_course_url_from_row(row)
        if require_probable_url and not _is_probable_sub_course_url(sub_url):
            continue

        slug_url = _build_slug_url(sub_url)
        slug_url = _normalize_slug_with_name(slug_url, name_text, parent_course_name)
        rating_text = _safe_text(name_td.locator("span.text-primary-black, span[class*='text-primary-black']"))
        reviews_text = _safe_text(name_td.locator("a:has-text('Review')"))
        fees_text = _safe_text(fees_td.locator("div.fs-16")) or _safe_text(fees_td)
        app_text = _safe_text(app_td.locator("div.fs-16")) or _safe_text(app_td)
        cutoff_text = _safe_text(cutoff_td.locator("div.fs-16")) if cutoff_td is not None else ""
        if not cutoff_text and cutoff_td is not None:
            cutoff_text = _safe_text(cutoff_td)

        name_text = _strip_noise(name_text)
        reviews_text = _strip_noise(reviews_text)
        fees_text = _strip_noise(fees_text)
        app_text = _strip_noise(app_text)
        cutoff_text = _strip_noise(cutoff_text)

        try:
            rating_val = float(re.search(r"\d+(\.\d+)?", rating_text).group(0)) if rating_text else None
        except Exception:
            rating_val = None
        if rating_val is not None and not (0 <= rating_val <= 5):
            rating_val = None

        reviews_val = _extract_reviews_count(reviews_text)

        parsed.append({
            "name": _typed_text(name_text) if name_text else None,
            "url": _typed_text(sub_url) if sub_url else None,
            "slug_url": _typed_text(slug_url) if slug_url else None,
            "rating": {"type": "text", "value": str(rating_val)} if rating_val is not None else None,
            "reviews": {"type": "text", "value": str(reviews_val)} if reviews_val is not None else None,
            "fees": _typed_text(fees_text) if fees_text else None,
            "application_date": _typed_text(app_text) if app_text else None,
            "cutoff": _typed_text(cutoff_text) if cutoff_text else None,
        })

    return parsed


def _strip_noise(text):
    cleaned = _clean(text)
    cleaned = re.sub(r"\bCheck\s*Details\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bCompare\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bRead\s*More\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bRead\s*Less\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


def _is_noise_text(text):
    t = _clean(text).lower()
    if not t:
        return True
    if re.fullmatch(r"\d{1,4}", t):
        return True
    if re.fullmatch(r"20\d{2}", t):
        return True
    noise_phrases = [
        "write a colleges review",
        "do you think the data is wrong",
        "how likely are you to recommend",
        "not so likely highly likely",
        "course finder",
        "search from 20k+ courses",
        "popular streams",
        "popular courses",
        "report here",
        "compare",
        "check ranking details",
        "check detailed fees",
        "view more",
        "show more",
        "show less",
        "students' opinion",
        "students opinion",
        "ai-generated summary",
        "personal ai",
        "collegedunia's personal ai",
        "report here",
        "yes no",
        "likes",
        "dislike",
        "reply",
        "share",
        "write a college review",
    ]
    if t in {"all", "prev", "next", "previous"}:
        return True
    return any(p in t for p in noise_phrases)


def _filter_typed_items(items):
    filtered = []
    seen_text = set()
    seen_links = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")

        if item_type == "text":
            text_val = _strip_noise(item.get("value", ""))
            if not text_val or _is_noise_text(text_val):
                continue
            dedupe_key = text_val.lower()
            if dedupe_key in seen_text:
                continue
            seen_text.add(dedupe_key)
            out = {"type": "text", "value": text_val}
            if item.get("value_html"):
                out["value_html"] = item.get("value_html")
            filtered.append(out)
            continue

        if item_type == "list":
            values = item.get("value", [])
            if isinstance(values, str):
                values = [values]
            clean_vals = []
            for v in values:
                vv = _strip_noise(v)
                if vv.lower() == "all":
                    continue
                if vv and not _is_noise_text(vv):
                    clean_vals.append(vv)
            if clean_vals:
                filtered.append({"type": "list", "value": clean_vals})
            continue

        if item_type == "table":
            rows = item.get("value", [])
            if rows:
                filtered.append(item)
            continue

        if item_type == "image":
            if item.get("src"):
                filtered.append(item)
            continue

        if item_type == "video":
            if item.get("src"):
                filtered.append(item)
            continue

        if item_type == "heading":
            hv = _strip_noise(item.get("value", ""))
            if hv and not _is_noise_text(hv):
                out = {"type": "heading", "value": hv}
                if item.get("level"):
                    out["level"] = item.get("level")
                if item.get("value_html"):
                    out["value_html"] = item.get("value_html")
                filtered.append(out)
            continue

        if item_type == "link":
            href = _clean(item.get("href", ""))
            label = _strip_noise(item.get("label", ""))
            if _is_noise_text(label):
                continue
            key = f"{href}::{label}".lower()
            if href and key not in seen_links:
                seen_links.add(key)
                filtered.append({"type": "link", "label": label, "href": href})
            continue

    # Post-process to merge split inline text and remove numeric pager garbage.
    merged = []
    i = 0
    while i < len(filtered):
        cur = filtered[i]
        if cur.get("type") == "text":
            txt = _clean(cur.get("value", ""))
            if txt.lower() in {"all", "view more", "show more", "show less", "previous", "next"}:
                i += 1
                continue

            # Merge patterns like "Application Date:" + "Aug 1, 2024 - Sep 13, 2025"
            if i + 1 < len(filtered):
                nxt = filtered[i + 1]
                if nxt.get("type") == "text":
                    n_txt = _clean(nxt.get("value", ""))
                    if txt.endswith(":") and n_txt:
                        merged.append({"type": "text", "value": f"{txt} {n_txt}"})
                        i += 2
                        continue
                    # Merge line fragments from same visual line.
                    if n_txt and txt and re.match(r"^[a-z]", n_txt) and not re.search(r"[.!?]$", txt):
                        merged.append({"type": "text", "value": f"{txt} {n_txt}"})
                        i += 2
                        continue
                    if re.match(r"^\d+\s+students?$", txt.lower()) and n_txt.lower().startswith("shown interest"):
                        merged.append({"type": "text", "value": f"{txt} {n_txt}"})
                        i += 2
                        continue

        merged.append(cur)
        i += 1

    # Drop numeric-only runs like 6,7,8,9,10 that are usually pagination/filter noise.
    final_items = []
    run = []
    for it in merged:
        if it.get("type") == "text" and re.fullmatch(r"\d{1,2}", _clean(it.get("value", "")) or ""):
            run.append(it)
            continue
        if run:
            if len(run) < 3:
                final_items.extend(run)
            run = []
        final_items.append(it)
    if run and len(run) < 3:
        final_items.extend(run)

    # Final strict pass for short junk fragments that survived earlier filters.
    strict = []
    for it in final_items:
        if it.get("type") == "text":
            v = _clean(it.get("value", ""))
            if _is_noise_text(v):
                continue
            if len(v) <= 2 and re.fullmatch(r"[a-zA-Z0-9]+", v):
                continue
        if it.get("type") == "list":
            vals = [x for x in it.get("value", []) if not _is_noise_text(x)]
            vals = [x for x in vals if not re.fullmatch(r"\d{1,4}", _clean(x))]
            if not vals:
                continue
            it = {"type": "list", "value": vals}
        strict.append(it)

    def _should_group_inline_run(run):
        if len(run) < 4:
            return False
        parts = []
        for x in run:
            if x.get("type") == "text":
                v = _clean(x.get("value", ""))
            else:
                v = _clean(x.get("label", ""))
            if not v:
                continue
            if len(v) > 110:
                return False
            parts.append(v.lower())
        if not parts:
            return False
        blob = " ".join(parts)
        signals = [
            "total fees",
            "fees",
            "reviews",
            "courses",
            "years",
            "full time",
            "part time",
            "eligibility",
            "application date",
            "median salary",
            "offered by",
        ]
        hit = sum(1 for s in signals if s in blob)
        return hit >= 2

    grouped = []
    i = 0
    while i < len(strict):
        cur = strict[i]
        if cur.get("type") not in {"text", "link"}:
            grouped.append(cur)
            i += 1
            continue

        j = i
        run = []
        while j < len(strict) and strict[j].get("type") in {"text", "link"}:
            run.append(strict[j])
            j += 1

        if _should_group_inline_run(run):
            lines = []
            for it in run:
                if it.get("type") == "text":
                    t = _clean(it.get("value", ""))
                    if t:
                        lines.append(t)
                else:
                    label = _clean(it.get("label", ""))
                    href = _clean(it.get("href", ""))
                    if label and "review" in label.lower() and lines and re.fullmatch(r"\d+(?:\.\d+)?", lines[-1]):
                        lines[-1] = f"{lines[-1]} {label}"
                    elif label and href:
                        lines.append(f"{label} ({href})")
                    elif label:
                        lines.append(label)
                    elif href:
                        lines.append(href)

            if lines:
                grouped.append({"type": "list", "value": lines})
            i = j
            continue

        grouped.extend(run)
        i = j

    return grouped


def _extract_typed_from_node(node):
    items = []
    try:
        items = node.evaluate(
            """(root) => {
                const out = [];
                const clean = (v) => (v || "").replace(/\\s+/g, " ").trim();
                const pushMedia = (el) => {
                    if (!el || !el.tagName) return;
                    const t = el.tagName.toLowerCase();
                    if (t === "img") {
                        const src = el.getAttribute("data-src") || el.getAttribute("src") || "";
                        if (src) out.push({ type: "image", src });
                    } else if (t === "iframe" || t === "video") {
                        let src = el.getAttribute("src") || el.getAttribute("data-src") || el.getAttribute("data-lazy-src") || "";
                        if (!src && t === "video") {
                            const s = el.querySelector("source");
                            src = s ? (s.getAttribute("src") || "") : "";
                        }
                        if (src) out.push({ type: "video", src });
                    }
                };
                const walk = (el) => {
                    if (!el || !el.tagName) return;
                    const tag = el.tagName.toLowerCase();
                    const cls = ((el.className || "") + "").toLowerCase();
                    const idv = ((el.id || "") + "").toLowerCase();

                    // Skip known UI/filter/feedback blocks that create garbage content.
                    if (
                        cls.includes("cutoff-filter") ||
                        cls.includes("filters-wrapper") ||
                        cls.includes("sticky-filter") ||
                        cls.includes("scroller") ||
                        cls.includes("clg-pill") ||
                        cls.includes("pills-container") ||
                        cls.includes("accuracy") ||
                        cls.includes("review-like-dislike") ||
                        cls.includes("like-dislike") ||
                        cls.includes("cta-btn") ||
                        cls.includes("course-finder") ||
                        idv.includes("likes-dislikes")
                    ) return;

                    if (["button", "input", "select", "option", "textarea", "label", "svg", "path"].includes(tag)) {
                        return;
                    }

                    if (tag === "img" || tag === "iframe" || tag === "video") {
                        pushMedia(el);
                        return;
                    }

                    if (/^h[2-6]$/.test(tag)) {
                        const value = clean(el.innerText);
                        if (value) {
                            const item = { type: "heading", level: tag, value };
                            const html = (el.innerHTML || "").trim();
                            if (html) item.value_html = html;
                            out.push(item);
                        }
                        return;
                    }

                    if (tag === "a") {
                        const href = el.getAttribute("href") || "";
                        const label = clean(el.innerText);
                        if (href) out.push({ type: "link", label, href });
                        for (const c of Array.from(el.children || [])) walk(c);
                        return;
                    }

                    if (tag === "p") {
                        for (const m of Array.from(el.querySelectorAll(":scope img, :scope iframe, :scope video"))) {
                            pushMedia(m);
                        }
                        const value = clean(el.innerText);
                        if (value) {
                            const item = { type: "text", value };
                            if (el.querySelector("strong, b")) {
                                const html = (el.innerHTML || "").trim();
                                if (html) item.value_html = html;
                            }
                            out.push(item);
                        }
                        return;
                    }

                    if (tag === "ul" || tag === "ol") {
                        const values = [];
                        for (const li of Array.from(el.querySelectorAll(":scope > li"))) {
                            const t = clean(li.innerText);
                            if (t) values.push(t);
                        }
                        if (values.length) out.push({ type: "list", value: values });
                        return;
                    }

                    if (tag === "table") {
                        const rows = [];
                        for (const tr of Array.from(el.querySelectorAll("tr"))) {
                            const cols = [];
                            for (const td of Array.from(tr.querySelectorAll("th, td"))) {
                                const t = clean(td.innerText);
                                if (t) cols.push(t);
                            }
                            if (cols.length) rows.push(cols);
                        }
                        if (rows.length) out.push({ type: "table", value: rows });
                        return;
                    }

                    const children = Array.from(el.children || []);
                    if (children.length) {
                        for (const c of children) walk(c);
                        return;
                    }

                    const value = clean(el.innerText);
                    if (value) out.push({ type: "text", value });
                };

                walk(root);
                return out;
            }"""
        ) or []
    except Exception:
        items = []

    for it in items:
        if isinstance(it, dict) and it.get("type") == "link":
            it["href"] = _absolute_url(it.get("href", ""))

    return _filter_typed_items(items)


def _is_security_locked(page):
    try:
        body_text = page.locator("body").inner_text().lower()
    except Exception:
        return False

    markers = [
        "access denied",
        "forbidden",
        "security check",
        "verify you are human",
        "cloudflare",
        "captcha",
        "request blocked",
    ]
    return any(m in body_text for m in markers)


def _expand_read_more(page):
    selectors = [
        "span:has-text('Read More')",
        "a:has-text('Read More')",
        "button:has-text('Read More')",
        "[data-csm-title='Read More']",
        ".college-page-read-more",
    ]

    for _ in range(6):
        clicked = 0
        for sel in selectors:
            btns = page.locator(sel)
            for i in range(btns.count()):
                btn = btns.nth(i)
                try:
                    if not btn.is_visible():
                        continue
                    btn.scroll_into_view_if_needed()
                    page.evaluate("(el) => el.click()", btn)
                    page.wait_for_timeout(250)
                    clicked += 1
                except Exception:
                    continue
        if clicked == 0:
            break

def _expand_read_more_in_scope(page, scope, rounds=5):
    selectors = [
        "span:has-text('Read More')",
        "a:has-text('Read More')",
        "button:has-text('Read More')",
        "span:has-text('View More')",
        "a:has-text('View More')",
        "button:has-text('View More')",
        "span:has-text('Show More')",
        "a:has-text('Show More')",
        "button:has-text('Show More')",
        "[data-csm-title='Read More']",
        ".college-page-read-more",
    ]

    for _ in range(rounds):
        clicked = 0
        for sel in selectors:
            try:
                btns = scope.locator(sel)
            except Exception:
                btns = page.locator(sel)
            for i in range(btns.count()):
                btn = btns.nth(i)
                try:
                    if not btn.is_visible():
                        continue
                    btn.scroll_into_view_if_needed()
                    page.evaluate("(el) => el.click()", btn)
                    page.wait_for_timeout(250)
                    clicked += 1
                except Exception:
                    continue
        if clicked == 0:
            break


def _get_content_root(page):
    # Priority order based on current DOM structure
    selectors = [
        ".cdcms_courses",
        "div[id='toc-table'] >> xpath=ancestor::section[1]",
        "main",
        "article",
        "body"
    ]

    for sel in selectors:
        loc = page.locator(sel)
        if loc.count() > 0:
            return loc.first

    return page.locator("body").first

def _collect_toc_items(page, root):
    link_titles = {}
    toc_items = []
    seen = set()

    toc_links = root.locator("#toc-table a[href*='#']")
    if toc_links.count() == 0:
        toc_links = page.locator("#toc-table a[href*='#']")
    if toc_links.count() == 0:
        toc_links = root.locator("a[href*='#'][data-college_section_name='article']")
    if toc_links.count() == 0:
        toc_links = page.locator("a[href*='#'][data-college_section_name='article']")
    anchor_ids = set()
    for i in range(toc_links.count()):
        href = (toc_links.nth(i).get_attribute("href") or "").strip()
        if "#" in href:
            anchor_ids.add(href.split("#")[1].strip())

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        href = (link.get_attribute("href") or "").strip()
        if "#" not in href:
            continue
        anchor = href.split("#", 1)[1].strip()
        if not anchor:
            continue
        title = _clean(link.inner_text())
        if anchor not in link_titles:
            link_titles[anchor] = title or anchor
        if anchor in seen:
            continue
        seen.add(anchor)
        toc_items.append({"anchor": anchor, "title": link_titles[anchor]})

    # Fallback only when no TOC links are present.
    if not toc_items:
        heading_nodes = root.locator("[id].scroll-offset, h2[id], h3[id], h4[id], h5[id]").element_handles()
        if len(heading_nodes) == 0:
            heading_nodes = page.locator("[id].scroll-offset, h2[id], h3[id], h4[id], h5[id]").element_handles()
        for h in heading_nodes:
            try:
                anchor = (h.get_attribute("id") or "").strip()
                title = _clean(h.inner_text())
            except Exception:
                continue
            if not anchor or anchor in seen:
                continue
            seen.add(anchor)
            toc_items.append({"anchor": anchor, "title": link_titles.get(anchor, title or anchor)})

    return toc_items


def _scroll_to_toc(page):
    for _ in range(8):
        toc = page.locator("#toc-table")
        if toc.count() > 0:
            toc.first.scroll_into_view_if_needed()
            page.wait_for_timeout(400)
            return True
        page.mouse.wheel(0, 1200)
        page.wait_for_timeout(300)
    return False

def _is_noise_section_title(title):
    t = _clean(title).lower()
    if not t:
        return True
    blocked = [
        "review",
        "students' opinion",
        "students opinion",
        "why to join",
        "news",
        "latest updates",
        "qna",
    ]
    return any(b in t for b in blocked)

def _post_filter_section_content(section_title, section_content):
    filtered = []
    for item in section_content:
        if not isinstance(item, dict):
            continue
        filtered.append(item)
    return filtered

def _expand_accordion_in_scope(page, scope, rounds=4):
    selectors = [
        "span[class*='arrow']",
        "[class*='arrow-d-black']",
        "[class*='icon-24']",
        "div[class*='accordion-item'] > div:first-child",
        "div[class*='accordion-item'] div[class*='justify-content-between']",
        "div[class*='accordian'] div[class*='justify-content-between']",
    ]

    for _ in range(rounds):
        clicked = 0
        for sel in selectors:
            try:
                nodes = scope.locator(sel)
            except Exception:
                nodes = page.locator(sel)
            for i in range(nodes.count()):
                n = nodes.nth(i)
                try:
                    if not n.is_visible():
                        continue
                    # Skip already rotated/open arrows when detectable.
                    cls = (n.get_attribute("class") or "").lower()
                    if "rotate-180" in cls:
                        continue
                    n.scroll_into_view_if_needed()
                    try:
                        n.click(timeout=1200)
                    except Exception:
                        page.evaluate("(el) => el.click()", n)
                    page.wait_for_timeout(200)
                    clicked += 1
                except Exception:
                    continue
        if clicked == 0:
            break

def _expand_admission_process_accordion(page, scope):
    # Specific fallback for the section shown in screenshot.
    rows = scope.locator(
        "div[class*='accordion-item'] div[class*='justify-content-between'], "
        "div[class*='accordian-item'] div[class*='justify-content-between']"
    )
    for i in range(rows.count()):
        row = rows.nth(i)
        try:
            if not row.is_visible():
                continue
            row.scroll_into_view_if_needed()
            try:
                row.click(timeout=1200)
            except Exception:
                page.evaluate("(el) => el.click()", row)
            page.wait_for_timeout(220)
        except Exception:
            continue

def _expand_important_dates_show_more(page, scope, max_clicks=20):
    for _ in range(max_clicks):
        try:
            btns = scope.locator("button:has-text('Show More'), span:has-text('Show More'), a:has-text('Show More')")
            if btns.count() == 0:
                break
            btn = btns.first
            if not btn.is_visible():
                break
            btn.scroll_into_view_if_needed()
            page.evaluate("(el) => el.click()", btn)
            page.wait_for_timeout(450)
        except Exception:
            break

def _activate_important_dates_all_filter(page, scope):
    selectors = [
        "button:has-text('All')",
        "span:has-text('All')",
        "a:has-text('All')",
        "[data-csm-title='All']",
        "[data-ga-title='All']",
    ]
    for sel in selectors:
        try:
            nodes = scope.locator(sel)
        except Exception:
            nodes = page.locator(sel)
        for i in range(nodes.count()):
            n = nodes.nth(i)
            try:
                if not n.is_visible():
                    continue
                n.scroll_into_view_if_needed()
                page.evaluate("(el) => el.click()", n)
                page.wait_for_timeout(350)
                return True
            except Exception:
                continue
    return False

def _should_walk_siblings_from_target(target, anchor):
    try:
        tag = (target.evaluate("e => (e.tagName || '').toLowerCase()") or "").strip()
    except Exception:
        tag = ""
    if tag in {"h2", "h3", "h4", "h5"}:
        return True
    try:
        tid = _clean(target.get_attribute("id") or "").lower()
    except Exception:
        tid = ""
    # If anchor resolves to a section/container div itself, don't walk outside.
    if tid and tid == _clean(anchor).lower():
        return False
    return False

def _is_course_finder_node(el):
    try:
        cls = (el.get_attribute("class") or "").lower()
    except Exception:
        cls = ""
    try:
        node_id = (el.get_attribute("id") or "").lower()
    except Exception:
        node_id = ""
    try:
        txt = _clean(el.inner_text()).lower()
    except Exception:
        txt = ""

    if "coursefinder" in cls or "course-finder" in cls:
        return True
    if "coursefinder" in node_id or "course-finder" in node_id:
        return True
    if "course finder" in txt and len(txt) < 800:
        return True
    return False

def _is_course_finder_visible(page):
    selectors = [
        "div[class*='coursefinder']",
        "div[class*='course-finder']",
        "section:has-text('Course Finder')",
        "div:has(> h2:has-text('Course Finder'))",
        "div:has-text('Search from 20K+ Courses')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                return True
        except Exception:
            continue
    return False


def _resolve_anchor_target(page, anchor):
    anchor = _clean(anchor).lstrip("#")
    if not anchor:
        return None

    for sel in (f"#{anchor}", f"[id='{anchor}']", f'[id="{anchor}"]'):
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                return loc.first
        except Exception:
            continue

    try:
        candidates = page.locator("[id]")
        for i in range(candidates.count()):
            node = candidates.nth(i)
            node_id = _clean(node.get_attribute("id") or "")
            if node_id.lower() == anchor.lower():
                return node
    except Exception:
        pass

    return None

def _scrape_toc_sections_from_current_page(page):
    detail = {
        "url": page.url,
        "about": [],
        "toc_sections": [],
    }

    if _is_security_locked(page):
        detail["skipped_reason"] = "security_lock"
        return detail

    page.wait_for_timeout(1200)

    _expand_read_more(page)

    _scroll_to_toc(page)

    toc_links = page.locator("#toc-table a[href*='#']")
    if toc_links.count() == 0:
        detail["skipped_reason"] = "toc_not_found"
        return detail

    anchor_ids = set()
    for i in range(toc_links.count()):
        href = (toc_links.nth(i).get_attribute("href") or "").strip()
        if "#" in href:
            anchor_ids.add(href.split("#", 1)[1].strip().lower())

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        href = (link.get_attribute("href") or "").strip()
        if "#" not in href:
            continue

        anchor = href.split("#", 1)[1].strip()
        section_title = _clean(link.inner_text())
        if _is_noise_section_title(section_title):
            continue

        # CLICK TOC LINK
        try:
            link.scroll_into_view_if_needed()
            page.evaluate("(el) => el.click()", link)
            page.wait_for_timeout(600)
        except Exception:
            continue

        # Locate section heading
        target = _resolve_anchor_target(page, anchor)
        if target is None:
            continue

        try:
            target.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
        except Exception:
            pass

        _expand_accordion_in_scope(page, target)
        _expand_admission_process_accordion(page, target)
        _expand_read_more_in_scope(page, target)
        if "important date" in section_title.lower():
            _activate_important_dates_all_filter(page, target)
            _expand_important_dates_show_more(page, target)
        _expand_read_more(page)
        page.wait_for_timeout(300)

        section_content = []

        # Include only heading label, not full container text.
        heading_text = ""
        try:
            tag_name = (target.evaluate("e => e.tagName.toLowerCase()") or "").strip()
            if tag_name in ["h2", "h3", "h4", "h5"]:
                heading_text = _clean(target.inner_text())
            else:
                h = target.query_selector("h2, h3, h4, h5")
                if h:
                    heading_text = _clean(h.inner_text())
        except Exception:
            heading_text = ""
        if heading_text:
            section_content.append({"type": "text", "value": heading_text})

        # Parse target block itself first.
        first_chunk = _extract_typed_from_node(target)
        if first_chunk:
            section_content.extend(first_chunk)

        if _should_walk_siblings_from_target(target, anchor):
            # Collect siblings until next heading
            current = target.evaluate_handle("el => el.nextElementSibling")

            while current:
                try:
                    tag = current.evaluate("e => e.tagName.toLowerCase()")
                    next_id = _clean(current.get_attribute("id") or "")
                except Exception:
                    break

                next_id_l = next_id.lower()
                if next_id_l in {"whatsnew", "toc-table"}:
                    break
                if _is_course_finder_node(current):
                    break
                if tag in {"h2", "h3"} and section_content:
                    break

                # Stop when next TOC anchor section starts.
                if next_id and next_id_l != anchor.lower() and next_id_l in anchor_ids:
                    break

                chunk = _extract_typed_from_node(current)
                if chunk:
                    section_content.extend(chunk)
                if "important date" in section_title.lower():
                    _expand_important_dates_show_more(page, current)
                    _expand_accordion_in_scope(page, current)

                current = current.evaluate_handle("e => e.nextElementSibling")

        section_content = _filter_typed_items(section_content)
        section_content = _post_filter_section_content(section_title, section_content)
        if section_content:
            detail["toc_sections"].append({
                "section": section_title,
                "content": section_content
            })
            if "ranking" in section_title.lower():
                break

    return detail

def _fetch_course_detail(page, course_url, max_sections=0, detail_page=None):
    detail = {"url": course_url, "page_heading": "", "about": [], "toc_sections": []}

    if not course_url:
        detail["skipped_reason"] = "missing_url"
        return detail

    temp = detail_page if detail_page is not None else page.context.new_page()
    created_here = detail_page is None

    try:
        temp.goto(course_url, wait_until="domcontentloaded", timeout=60000)
        detail["page_heading"] = _safe_text(temp.locator("h1#collegePageTitle, h1"))

        # Force full render.
        for _ in range(6):
            temp.mouse.wheel(0, 2500)
            temp.wait_for_timeout(220)
            if _is_course_finder_visible(temp):
                break

        temp.wait_for_timeout(650)
        _expand_read_more(temp)

        # Wait for TOC and collect links first.
        temp.wait_for_selector("#toc-table a[href*='#']", timeout=5000)
        toc_links = temp.locator("#toc-table a[href*='#']")

        toc_items = []
        seen_anchors = set()
        anchor_ids = set()
        for i in range(toc_links.count()):
            link = toc_links.nth(i)
            href = (link.get_attribute("href") or "").strip()
            if "#" not in href:
                continue
            anchor = href.split("#", 1)[1].strip()
            if not anchor:
                continue
            if anchor.lower() in seen_anchors:
                continue
            seen_anchors.add(anchor.lower())
            title = _clean(link.inner_text()) or anchor
            toc_items.append({"anchor": anchor, "title": title})
            anchor_ids.add(anchor.lower())

        if not toc_items:
            detail["skipped_reason"] = "toc_not_found"
            return detail

        sections_scraped = 0
        for toc_item in toc_items:
            if isinstance(max_sections, int) and max_sections > 0 and sections_scraped >= max_sections:
                break
            anchor = toc_item["anchor"]
            section_title = toc_item["title"]
            if _is_noise_section_title(section_title):
                continue

            # Click TOC item to trigger lazy render.
            try:
                link = temp.locator(f"#toc-table a[href$='#{anchor}']")
                if link.count() == 0:
                    link = temp.locator(f"#toc-table a[href*='#{anchor}']")
                if link.count() > 0:
                    link.first.scroll_into_view_if_needed()
                    try:
                        link.first.click(timeout=3000)
                    except Exception:
                        temp.evaluate("(el) => el.click()", link.first)
                else:
                    temp.evaluate(f"window.location.hash = '#{anchor}'")
                temp.wait_for_timeout(280)
            except Exception:
                try:
                    temp.evaluate(f"window.location.hash = '#{anchor}'")
                    temp.wait_for_timeout(220)
                except Exception:
                    continue

            target = _resolve_anchor_target(temp, anchor)
            if target is None:
                continue

            try:
                target.scroll_into_view_if_needed()
                temp.wait_for_timeout(140)
            except Exception:
                pass

            _expand_accordion_in_scope(temp, target)
            _expand_admission_process_accordion(temp, target)
            _expand_read_more_in_scope(temp, target)
            if "important date" in section_title.lower():
                _activate_important_dates_all_filter(temp, target)
                _expand_important_dates_show_more(temp, target)
            _expand_read_more(temp)
            temp.wait_for_timeout(120)

            section_content = []
            heading_text = ""
            try:
                tag_name = (target.evaluate("e => e.tagName.toLowerCase()") or "").strip()
                if tag_name in ["h2", "h3", "h4", "h5"]:
                    heading_text = _clean(target.inner_text())
                else:
                    h = target.query_selector("h2, h3, h4, h5")
                    if h:
                        heading_text = _clean(h.inner_text())
            except Exception:
                heading_text = ""
            if heading_text:
                section_content.append({"type": "text", "value": heading_text})

            first_chunk = _extract_typed_from_node(target)
            if first_chunk:
                section_content.extend(first_chunk)

            if _should_walk_siblings_from_target(target, anchor):
                # Walk sibling nodes until next TOC section / known stop blocks.
                current = target.evaluate_handle("el => el.nextElementSibling")
                while current:
                    try:
                        tag = current.evaluate("e => e.tagName.toLowerCase()")
                        next_id = _clean(current.get_attribute("id") or "")
                    except Exception:
                        break

                    next_id_l = next_id.lower()
                    if next_id_l in {"whatsnew", "toc-table"}:
                        break
                    if _is_course_finder_node(current):
                        break
                    if tag in {"h2", "h3"} and section_content:
                        break
                    if next_id and next_id_l != anchor.lower() and next_id_l in anchor_ids:
                        break

                    chunk = _extract_typed_from_node(current)
                    if chunk:
                        section_content.extend(chunk)
                    if "important date" in section_title.lower():
                        _expand_important_dates_show_more(temp, current)
                        _expand_accordion_in_scope(temp, current)

                    current = current.evaluate_handle("e => e.nextElementSibling")

            section_content = _filter_typed_items(section_content)
            section_content = _post_filter_section_content(section_title, section_content)
            if section_content:
                detail["toc_sections"].append({
                    "section": section_title,
                    "content": section_content
                })
                sections_scraped += 1
                if "ranking" in section_title.lower():
                    break

    except Exception as e:
        detail["skipped_reason"] = str(e)

    finally:
        if created_here:
            temp.close()

    return detail
def _enrich_sub_courses_with_details(
    page,
    sub_courses,
    limit_sub_courses=None,
    max_sections=0,
    parent_label="",
    detail_cache=None,
    detail_page=None,
):
    target_list = sub_courses

    total = len(target_list)
    for idx, sub in enumerate(target_list, start=1):
        sub_url = ""
        raw_url = sub.get("url")
        if isinstance(raw_url, dict):
            sub_url = _clean(raw_url.get("value"))
        elif isinstance(raw_url, str):
            sub_url = _clean(raw_url)

        if sub_url:
            sub_name = ""
            if isinstance(sub.get("name"), dict):
                sub_name = _clean(sub["name"].get("value"))
            elif isinstance(sub.get("name"), str):
                sub_name = _clean(sub.get("name"))

            if isinstance(detail_cache, dict) and sub_url in detail_cache:
                sub["course_detail"] = detail_cache[sub_url]
                print(
                    f"  [sub-detail {idx}/{total}] cache hit for {sub_name or sub_url}",
                    flush=True,
                )
                continue

            print(
                f"  [sub-detail {idx}/{total}] {parent_label} -> {sub_name or sub_url}",
                flush=True,
            )
            started = time.time()
            sub["course_detail"] = _fetch_course_detail(
                page,
                sub_url,
                max_sections=max_sections,
                detail_page=detail_page,
            )
            if isinstance(detail_cache, dict):
                detail_cache[sub_url] = sub["course_detail"]
            elapsed = time.time() - started
            sec_count = len(sub["course_detail"].get("toc_sections", []))
            print(
                f"  [sub-detail {idx}/{total}] done in {elapsed:.1f}s, sections={sec_count}",
                flush=True,
            )
        else:
            sub["course_detail"] = {"about": [], "toc_sections": [], "skipped_reason": "missing_url"}
    return target_list


def _parse_sub_courses_from_course_page(
    page,
    course_href,
    parent_course_name,
    detail_page=None,
    expected_sub_count=None,
):
    if not course_href:
        return []

    temp = detail_page if detail_page is not None else page.context.new_page()
    created_here = detail_page is None
    best_sub_courses = []
    try:
        temp.goto(course_href, wait_until="domcontentloaded", timeout=45000)
        temp.wait_for_timeout(550)
        best_sub_courses = _collect_sub_courses_from_scope(
            temp,
            temp,
            selectors=["table tbody"],
            parent_course_name=parent_course_name,
            expected_sub_count=expected_sub_count,
            require_probable_url=True,
        )
    except Exception:
        best_sub_courses = []
    finally:
        if created_here:
            try:
                temp.close()
            except Exception:
                pass

    return best_sub_courses


def _parse_sub_courses_from_card(
    page,
    card,
    allow_detail_page_fallback=False,
    detail_page=None,
    expected_sub_count=None,
):
    sub_courses = []
    parent_course_name = _safe_text(card.locator("div.course-detail a"))

    # Expand only if this card has sub-courses.
    view_btn = card.locator("button:has-text('View')")
    course_href = ""
    try:
        course_href = card.locator("div.course-detail a").first.get_attribute("href") or ""
    except Exception:
        course_href = ""
    course_href = _absolute_url(course_href)

    if view_btn.count() > 0:
        for _ in range(5):
            _safe_click(page, view_btn)

            page.mouse.wheel(0, 800)
            page.wait_for_timeout(400)

            try:
                card.locator("div.course-other-detail table tbody").first.wait_for(timeout=5000)
                break
            except:
                page.wait_for_timeout(800)
    best_rows = _collect_sub_courses_from_scope(
        page,
        card,
        selectors=[
            "div.course-other-detail table tbody",
            "table tbody",
        ],
        parent_course_name=parent_course_name,
        expected_sub_count=expected_sub_count,
        require_probable_url=False,
    )

    if best_rows:
        if (
            course_href
            and allow_detail_page_fallback
            and isinstance(expected_sub_count, int)
            and expected_sub_count > 0
            and len(best_rows) < expected_sub_count
        ):
            fallback_rows = _parse_sub_courses_from_course_page(
                page,
                course_href,
                parent_course_name,
                detail_page=detail_page,
                expected_sub_count=expected_sub_count,
            )
            if len(fallback_rows) > len(best_rows):
                return fallback_rows
        return best_rows

    if course_href and allow_detail_page_fallback:
        return _parse_sub_courses_from_course_page(
            page,
            course_href,
            parent_course_name,
            detail_page=detail_page,
            expected_sub_count=expected_sub_count,
        )

    return sub_courses


def scrape_courses_fees_cards(
    page,
    limit_courses=None,
    limit_sub_courses=None,
    fetch_course_detail=True,
    fetch_sub_course_detail=True,
    max_sections_per_course=0,
    allow_sub_course_fallback=False,
    source_college_id=None,
):
    _scroll_to_course_cards(page)

    cards = page.locator("section div[class*='course-card'], div.course-card")
    results = []
    course_detail_cache = {}
    sub_course_detail_cache = {}

    max_cards = cards.count()
    if isinstance(limit_courses, int) and limit_courses > 0:
        max_cards = min(max_cards, limit_courses)

    print(
        f"[start] Found {cards.count()} course cards. Processing {max_cards}. "
        f"fetch_course_detail={fetch_course_detail}, fetch_sub_course_detail={fetch_sub_course_detail}",
        flush=True,
    )

    run_started = time.time()
    detail_worker_page = None
    if fetch_course_detail or fetch_sub_course_detail or allow_sub_course_fallback:
        detail_worker_page = page.context.new_page()

    try:
        for i in range(max_cards):
            card = cards.nth(i)
            course_started = time.time()
            try:
                card.scroll_into_view_if_needed()
                item = _extract_card_fields(card)
                item["college_id"] = source_college_id
                course_name = item.get("course_name") or "unknown-course"

                print(f"[course {i + 1}/{max_cards}] {course_name}", flush=True)

                if fetch_course_detail:
                    main_url = item.get("course_url", "")
                    if main_url in course_detail_cache:
                        item["course_detail"] = course_detail_cache[main_url]
                        print(f"[course {i + 1}/{max_cards}] course detail cache hit", flush=True)
                    else:
                        detail_started = time.time()
                        item["course_detail"] = _fetch_course_detail(
                            page,
                            main_url,
                            max_sections=max_sections_per_course,
                            detail_page=detail_worker_page,
                        )
                        course_detail_cache[main_url] = item["course_detail"]
                        detail_elapsed = time.time() - detail_started
                        sec_count = len(item["course_detail"].get("toc_sections", []))
                        print(
                            f"[course {i + 1}/{max_cards}] course detail done in {detail_elapsed:.1f}s, sections={sec_count}",
                            flush=True,
                        )

                if item.get("sub_course_count"):
                    expected_sub_count = item.get("sub_course_count")
                    sub_courses = _parse_sub_courses_from_card(
                        page,
                        card,
                        allow_detail_page_fallback=allow_sub_course_fallback,
                        detail_page=detail_worker_page,
                        expected_sub_count=expected_sub_count,
                    )
                    recovered_sub_courses = _recover_sub_courses_from_course_detail(
                        item.get("course_detail", {}),
                        parent_course_name=course_name,
                        expected_count=expected_sub_count,
                    )
                    if recovered_sub_courses:
                        merged_sub_courses = _merge_sub_course_urls_from_recovered(
                            sub_courses,
                            recovered_sub_courses,
                        )
                        if merged_sub_courses != sub_courses:
                            print(
                                f"[course {i + 1}/{max_cards}] merged url fields from course_detail",
                                flush=True,
                            )
                        sub_courses = merged_sub_courses

                    if _needs_sub_course_recovery(sub_courses, expected_count=expected_sub_count) and recovered_sub_courses:
                        if not sub_courses:
                            print(
                                f"[course {i + 1}/{max_cards}] recovered sub-courses from course_detail={len(recovered_sub_courses)}",
                                flush=True,
                            )
                            sub_courses = recovered_sub_courses
                        else:
                            print(
                                f"[course {i + 1}/{max_cards}] skip replacing sub-courses: recovered {len(recovered_sub_courses)} < expected {expected_sub_count}",
                                flush=True,
                            )
                    print(
                        f"[course {i + 1}/{max_cards}] parsed sub-courses={len(sub_courses)}",
                        flush=True,
                    )
                    if fetch_sub_course_detail and sub_courses:
                        sub_courses = _enrich_sub_courses_with_details(
                            page,
                            sub_courses,
                            limit_sub_courses=limit_sub_courses,
                            max_sections=max_sections_per_course,
                            parent_label=course_name,
                            detail_cache=sub_course_detail_cache,
                            detail_page=detail_worker_page,
                        )
                    item["sub_courses"] = sub_courses

                results.append(item)
                elapsed = time.time() - course_started
                print(f"[course {i + 1}/{max_cards}] done in {elapsed:.1f}s", flush=True)
            except Exception as e:
                print(f"[course {i + 1}/{max_cards}] skipped due to error: {e}", flush=True)
                continue
    finally:
        if detail_worker_page is not None:
            try:
                detail_worker_page.close()
            except Exception:
                pass

    # Remove empty artifacts
    cleaned = []
    for row in results:
        if row.get("course_name"):
            cleaned.append(row)

    total_elapsed = time.time() - run_started
    print(f"[done] Scraped {len(cleaned)} courses in {total_elapsed:.1f}s", flush=True)

    return cleaned


def save_to_mongo(output):
    client = MongoClient(MONGO_URI)
    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]
        coll.replace_one(
            {"college_id": output.get("college_id"), "url": output.get("url")},
            output,
            upsert=True,
        )
    finally:
        client.close()


def main():
    args = _parse_args()
    run_started = time.time()
    target_url = _resolve_runtime_url(args.url)
    output_file = _resolve_output_file(args.output_file)

    output = {
        "source": "collegedunia",
        "url": target_url,
        "college_id": _extract_college_id(target_url),
        "courses": [],
    }

    print(
        "[config] "
        f"headless={args.headless}, slow_mo={args.slow_mo}, "
        f"limit_courses={args.limit_courses}, limit_sub_courses={args.limit_sub_courses}, "
        f"fetch_course_detail={args.fetch_course_detail}, "
        f"fetch_sub_course_detail={args.fetch_sub_course_detail}, "
        f"max_sections_per_course={args.max_sections_per_course}",
        flush=True,
    )

    with sync_playwright() as p:
        browser = _launch_browser(
            p,
            headless=args.headless,
            slow_mo=args.slow_mo,
        )
        context_kwargs = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121",
            "viewport": {"width": 1440, "height": 1000},
        }
        if _should_use_low_memory_mode():
            context_kwargs["service_workers"] = "block"

        context = browser.new_context(**context_kwargs)
        if _should_use_low_memory_mode():
            context.route("**/*", _route_handler)
        page = context.new_page()

        try:
            open_courses_fees(page, target_url)
            output["courses"] = scrape_courses_fees_cards(
                page,
                limit_courses=args.limit_courses,
                limit_sub_courses=args.limit_sub_courses,
                fetch_course_detail=args.fetch_course_detail,
                fetch_sub_course_detail=args.fetch_sub_course_detail,
                max_sections_per_course=args.max_sections_per_course,
                allow_sub_course_fallback=True,
                source_college_id=output["college_id"],
            )
        finally:
            try:
                context.close()
            except Exception:
                pass
            browser.close()

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    save_to_mongo(output)

    total_elapsed = time.time() - run_started
    print(f"Saved {len(output['courses'])} course cards to {output_file}")
    print(f"Data upserted to MongoDB collection: {MONGO_COLLECTION}")
    print(f"Total runtime: {total_elapsed:.1f}s")


if __name__ == "__main__":
    main()
