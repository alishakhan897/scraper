import argparse
import json
import os
import time
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from pymongo import MongoClient


DEFAULT_COURSES_URL = "https://collegedunia.com/courses"
COURSES_URL = DEFAULT_COURSES_URL
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://alishakhan8488_db_user:DaVHn9goL8STNzNs@cluster0.nkmbpqt.mongodb.net/studentcap?retryWrites=true&w=majority",
)
MONGO_DB = os.getenv("MONGO_DB", "studentcap")
MONGO_COLLECTION = os.getenv("SCRAPER_COURSE_MONGO_COLLECTION", "maincourse")
DEFAULT_OUTPUT_FILE = "engineering_courses.json"
OUTPUT_FILE = DEFAULT_OUTPUT_FILE
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
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


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Scrape Collegedunia course streams or a single stream URL."
    )
    parser.add_argument(
        "--url",
        default="",
        help="Courses landing URL or a direct stream/all-courses URL.",
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
        help="Run browser in headless mode.",
    )
    parser.add_argument(
        "--headed",
        dest="headless",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--stream-limit",
        type=int,
        default=2,
        help="Limit streams to scrape when using the main /courses page (0 = no limit).",
    )
    parser.add_argument(
        "--course-limit",
        type=int,
        default=2,
        help="Limit courses scraped per stream (0 = no limit).",
    )
    parser.set_defaults(headless=_default_headless())
    return parser.parse_args()


def _resolve_runtime_url(cli_url=""):
    return (cli_url or os.getenv("SCRAPER_COURSE_URL") or DEFAULT_COURSES_URL).strip()


def _resolve_output_file(cli_output_file=""):
    return (
        cli_output_file
        or os.getenv("SCRAPER_COURSE_OUTPUT_FILE")
        or DEFAULT_OUTPUT_FILE
    ).strip()


def _limit_items(items, limit):
    if isinstance(limit, int) and limit > 0:
        return items[:limit]
    return items


def _is_root_courses_url(target_url):
    path = (urlparse(target_url).path or "").strip("/").lower()
    return path == "courses"


def _humanize_slug(slug):
    return " ".join(part.capitalize() for part in slug.replace("_", "-").split("-") if part)


def _safe_text(locator):
    try:
        if locator.count() > 0:
            return " ".join(locator.first.inner_text().split()).strip()
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
            target.click(timeout=3000)
        except Exception:
            page.evaluate("(el) => el.click()", target)
        return True
    except Exception:
        return False


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


def open_engineering_stream(page):
    page.goto(COURSES_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1200)

    # Scroll until the Engineering stream card is visible.
    for _ in range(8):
        card = page.locator("div.interestcard:has-text('Engineering')")
        if card.count() > 0 and card.first.is_visible():
            break
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(600)

    # Click "Explore all courses" for Engineering.
    engineering_link = page.locator(
        "div.interestcard:has-text('Engineering') a:has-text('Explore all courses')"
    )

    if engineering_link.count() == 0:
        # Fallback selector path.
        engineering_link = page.locator("a[href*='/courses/engineering']")

    if engineering_link.count() == 0:
        raise RuntimeError("Engineering 'Explore all courses' link not found")

    if not _safe_click(page, engineering_link):
        page.goto("https://collegedunia.com/courses/engineering", wait_until="networkidle", timeout=60000)
    else:
        page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1000)


def open_engineering_all_courses_tab(page):
    all_courses_tab = page.locator("a:has-text('All Courses')")
    if all_courses_tab.count() > 0:
        if not _safe_click(page, all_courses_tab):
            page.goto("https://collegedunia.com/courses/engineering/all-courses", wait_until="networkidle")
        else:
            page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1000)
        return

    # Fallback if tab is not clickable from UI.
    if "/courses/engineering" in page.url and "/all-courses" not in page.url:
        page.goto("https://collegedunia.com/courses/engineering/all-courses", wait_until="networkidle")
        page.wait_for_timeout(1000)


def _auto_scroll_all_courses(page):
    previous_count = -1
    stable_rounds = 0

    for _ in range(18):
        cards = page.locator("div.course-list")
        count = cards.count()

        if count <= previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            previous_count = count

        if stable_rounds >= 3:
            break

        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(700)


def scrape_engineering_all_courses(page):
    _auto_scroll_all_courses(page)

    results = []
    cards = page.locator("div.course-list")

    for i in range(cards.count()):
        card = cards.nth(i)

        name = _safe_text(card.locator("div.course-header h3 a"))
        if not name:
            name = _safe_text(card.locator("h3 a"))

        duration = _safe_text(card.locator("li.duration"))
        mode = _safe_text(card.locator("li.time"))

        if name:
            results.append(
                {
                    "course_name": name,
                    "duration": duration,
                    "mode": mode,
                }
            )

    return results


def open_first_engineering_course_detail(page):
    page.wait_for_selector("div.course-list", timeout=20000)
    first_card = page.locator("div.course-list").first
    if first_card.count() == 0:
        raise RuntimeError("No course card found on engineering all-courses page")

    first_link = first_card.locator("h3 a").first
    if first_link.count() == 0:
        raise RuntimeError("No course link found on engineering all-courses page")

    course_title = _safe_text(first_link)
    course_url = first_link.get_attribute("href") or ""
    course_duration = _safe_text(first_card.locator("li.duration"))
    course_mode = _safe_text(first_card.locator("li.time"))

    if course_url and course_url.startswith("/"):
        course_url = "https://collegedunia.com" + course_url

    current_url = page.url
    first_link.scroll_into_view_if_needed()
    _safe_click(page, first_link)
    try:
        page.wait_for_load_state("networkidle")
    except Exception:
        pass
    page.wait_for_timeout(1000)

    # Fallback: if click doesn't navigate (overlay/security/UI issue), open link directly.
    if page.url == current_url and course_url:
        try:
            page.goto(course_url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(1000)
        except Exception:
            pass

    return {
        "course_name": course_title,
        "course_url": course_url or page.url,
        "duration": course_duration,
        "mode": course_mode,
    }


def _absolute_url(url):
    value = (url or "").strip()
    if not value:
        return ""
    if value.startswith("//"):
        return "https:" + value
    if value.startswith("/"):
        return "https://collegedunia.com" + value
    return value


def _stream_slug_from_url(stream_url):
    path = (urlparse(stream_url).path or "").strip("/")
    parts = [part for part in path.split("/") if part]
    if len(parts) >= 2 and parts[0].lower() == "courses":
        return parts[1].strip().lower()
    return ""


def _build_stream_meta_from_url(target_url):
    absolute_url = _absolute_url(target_url).rstrip("/")
    parsed = urlparse(absolute_url)
    parts = [part for part in (parsed.path or "").split("/") if part]

    if len(parts) < 2 or parts[0].lower() != "courses":
        return None

    stream_slug = parts[1].strip().lower()
    if not stream_slug or stream_slug == "all-courses":
        return None

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    stream_url = f"{base_url}/courses/{stream_slug}"
    all_courses_url = f"{stream_url}/all-courses"

    if absolute_url.endswith("/all-courses"):
        all_courses_url = absolute_url

    return {
        "stream": stream_slug,
        "stream_name": _humanize_slug(stream_slug) or stream_slug,
        "stream_url": stream_url,
        "all_courses_url": all_courses_url,
    }


def _write_output_snapshot(data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _expand_show_more_streams(page):
    for _ in range(3):
        button = page.locator(
            "span[data-csm-title='SHOW MORE STREAMS'], span:has-text('Show more streams')"
        )
        clicked = False

        for i in range(button.count()):
            btn = button.nth(i)
            try:
                if not btn.is_visible():
                    continue
                btn.scroll_into_view_if_needed()
                if not _safe_click(page, btn):
                    page.evaluate("(el) => el.click()", btn)
                page.wait_for_timeout(1200)
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            break


def _collect_all_streams(page):
    page.goto(COURSES_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1200)

    for _ in range(8):
        page.mouse.wheel(0, 1600)
        page.wait_for_timeout(300)

    _expand_show_more_streams(page)

    cards = page.locator("div.interestcard")
    streams = []
    seen = set()

    for i in range(cards.count()):
        card = cards.nth(i)

        try:
            lines = [
                " ".join(line.split()).strip()
                for line in card.inner_text().splitlines()
                if " ".join(line.split()).strip()
            ]
        except Exception:
            lines = []

        stream_name = lines[0] if lines else ""
        link = card.locator("a:has-text('Explore all courses')")
        href = ""
        if link.count() > 0:
            href = link.first.get_attribute("href") or ""

        stream_url = _absolute_url(href)
        stream_slug = _stream_slug_from_url(stream_url)
        all_courses_url = stream_url.rstrip("/") + "/all-courses" if stream_url else ""
        key = stream_url or stream_name.lower()

        if not stream_name or not key or key in seen:
            continue

        seen.add(key)
        streams.append(
            {
                "stream": stream_slug or stream_name.lower().replace(" ", "-"),
                "stream_name": stream_name,
                "stream_url": stream_url,
                "all_courses_url": all_courses_url,
            }
        )

    return streams


def _open_stream_all_courses_page(page, stream_meta):
    stream_url = stream_meta.get("stream_url", "")
    all_courses_url = stream_meta.get("all_courses_url", "")
    target_url = stream_url or all_courses_url

    if not target_url:
        raise RuntimeError(f"Missing stream URL for {stream_meta.get('stream_name', 'unknown stream')}")

    page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1200)

    if _is_security_locked(page):
        return page.url

    if "/all-courses" in page.url:
        return page.url

    all_courses_tab = page.locator("a:has-text('All Courses')")
    if all_courses_tab.count() > 0:
        if not _safe_click(page, all_courses_tab):
            if all_courses_url:
                page.goto(all_courses_url, wait_until="domcontentloaded", timeout=60000)
        else:
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
    elif all_courses_url:
        page.goto(all_courses_url, wait_until="domcontentloaded", timeout=60000)

    page.wait_for_timeout(1000)
    return page.url


def _collect_current_stream_courses(page):
    _auto_scroll_all_courses(page)

    results = []
    seen = set()
    cards = page.locator("div.course-list")

    for i in range(cards.count()):
        card = cards.nth(i)
        link = card.locator("div.course-header h3 a")
        if link.count() == 0:
            link = card.locator("h3 a")

        name = _safe_text(link)
        href = ""
        if link.count() > 0:
            href = link.first.get_attribute("href") or ""

        course_url = _absolute_url(href)
        duration = _safe_text(card.locator("li.duration"))
        mode = _safe_text(card.locator("li.time"))
        key = course_url or name.lower()

        if not name or not key or key in seen:
            continue

        seen.add(key)
        results.append(
            {
                "course_name": name,
                "course_url": course_url,
                "duration": duration,
                "mode": mode,
            }
        )

    return results


def _scrape_course_payload(page, course_meta):
    course_data = dict(course_meta)
    course_url = course_meta.get("course_url", "")

    if not course_url:
        course_data["course_detail"] = {"skipped_reason": "missing_course_url"}
        course_data["syllabus_detail"] = {"skipped_reason": "missing_course_url"}
        return course_data

    try:
        page.goto(course_url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(1000)
        course_data["course_detail"] = scrape_single_course_detail(page)
    except PlaywrightTimeoutError:
        course_data["course_detail"] = {"skipped_reason": "timeout_on_course_detail"}
    except Exception as exc:
        course_data["course_detail"] = {"skipped_reason": f"course_detail_error: {type(exc).__name__}"}

    try:
        if open_syllabus_page(page, course_url):
            course_data["syllabus_detail"] = scrape_syllabus_detail(page)
        else:
            course_data["syllabus_detail"] = {"skipped_reason": "missing_course_url"}
    except PlaywrightTimeoutError:
        course_data["syllabus_detail"] = {"skipped_reason": "timeout_on_syllabus_detail"}
    except Exception as exc:
        course_data["syllabus_detail"] = {"skipped_reason": f"syllabus_detail_error: {type(exc).__name__}"}

    course_data["final_url"] = page.url
    return course_data


def _scrape_stream_payload(page, stream_meta, course_limit=2):
    stream_data = {
        "stream": stream_meta.get("stream", ""),
        "stream_name": stream_meta.get("stream_name", ""),
        "source_url": stream_meta.get("stream_url", "") or COURSES_URL,
        "stream_url": stream_meta.get("stream_url", ""),
        "all_courses_url": stream_meta.get("all_courses_url", ""),
        "courses": [],
    }

    # open stream → all courses page
    current_all_courses_url = _open_stream_all_courses_page(page, stream_meta)

    stream_data["all_courses_url"] = current_all_courses_url or stream_data["all_courses_url"]
    stream_data["source_url"] = stream_data["all_courses_url"] or stream_data["source_url"]

    if _is_security_locked(page):
        stream_data["skipped_reason"] = "security_lock"
        stream_data["final_url"] = page.url
        return stream_data

    # collect all course cards
    course_metas = _limit_items(_collect_current_stream_courses(page), course_limit)
    stream_data["course_total"] = len(course_metas)

    print(
        f"[{stream_data['stream_name']}] total courses found: {len(course_metas)}",
        flush=True,
    )

    # iterate every course
    for index, course_meta in enumerate(course_metas, start=1):

        course_name = course_meta.get("course_name") or "unknown-course"

        print(
            f"[stream {stream_data['stream_name']}] course {index}/{len(course_metas)} → {course_name}",
            flush=True,
        )

        try:
            # scrape course detail + syllabus
            course_payload = _scrape_course_payload(page, course_meta)

        except Exception as exc:
            course_payload = {
                **course_meta,
                "course_detail": {"skipped_reason": f"course_error: {type(exc).__name__}"},
                "syllabus_detail": {"skipped_reason": f"course_error: {type(exc).__name__}"},
            }

        course_payload["course_index"] = index
        stream_data["courses"].append(course_payload)

        # -----------------------------
        # IMPORTANT: go back to all courses page
        # -----------------------------
        try:
            page.goto(stream_data["all_courses_url"], wait_until="domcontentloaded", timeout=60000)

            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            page.wait_for_timeout(1200)

            # scroll again to load all cards
            _auto_scroll_all_courses(page)

        except Exception as exc:
            print("Failed returning to course list:", exc)

        # small delay to avoid Cloudflare blocking
        time.sleep(2)

    stream_data["final_url"] = page.url

    return stream_data

def _extract_typed_content(el):
    content = []

    def parse(node):
        try:
            tag = node.evaluate("n => n.tagName ? n.tagName.toLowerCase() : null")
        except Exception:
            return

        if not tag:
            return

        if tag == "img":
            src = node.get_attribute("data-src") or node.get_attribute("src")
            if src:
                content.append({"type": "image", "src": src})
            return

        if tag == "p":
            txt = " ".join(node.inner_text().split()).strip()
            if txt:
                content.append({"type": "text", "value": txt})
            for img in node.query_selector_all("img"):
                src = img.get_attribute("data-src") or img.get_attribute("src")
                if src:
                    content.append({"type": "image", "src": src})
            return

        if tag in ["ul", "ol"]:
            items = []
            for li in node.query_selector_all("li"):
                t = " ".join(li.inner_text().split()).strip()
                if t:
                    items.append(t)
            if items:
                content.append({"type": "list", "value": items})
            return

        if tag == "table":
            rows = []
            for tr in node.query_selector_all("tr"):
                cols = []
                for cell in tr.query_selector_all("th, td"):
                    t = " ".join(cell.inner_text().split()).strip()
                    if t:
                        cols.append(t)
                if cols:
                    rows.append(cols)
            if rows:
                content.append({"type": "table", "value": rows})
            return

        if tag in ["div", "section", "article"]:
            for child in node.query_selector_all(":scope > *"):
                parse(child)
            return

        txt = " ".join(node.inner_text().split()).strip()
        if txt:
            content.append({"type": "text", "value": txt})

    parse(el)
    return content


def _expand_course_read_more(page):
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
                    page.wait_for_timeout(350)
                    clicked += 1
                except Exception:
                    continue
        if clicked == 0:
            break


def _extract_faq_qa_content(page, target_heading):
    content = []

    # Find FAQ container near current section heading (usually next siblings).
    el = target_heading.evaluate_handle("el => el.nextElementSibling")
    faq_container = None

    while el:
        try:
            tag = el.evaluate("el => el.tagName.toLowerCase()")
            el_id = (el.get_attribute("id") or "").lower()
            el_class = (el.get_attribute("class") or "").lower()
        except Exception:
            break

        if tag == "h2":
            break

        if "cdcms_faqs" in el_class or el_id == "faq_id":
            faq_container = el
            break

        # Sometimes FAQ id is nested one level deeper.
        try:
            nested = el.query_selector("#faq_id, .cdcms_faqs")
            if nested:
                faq_container = nested
                break
        except Exception:
            pass

        el = el.evaluate_handle("el => el.nextElementSibling")

    # Fallback: use global FAQ containers if local traversal didn't find one.
    if not faq_container:
        global_faq = page.locator("#faq_id, .cdcms_faqs")
        if global_faq.count() > 0:
            faq_container = global_faq.first

    if not faq_container:
        return content

    questions = []
    answers = []
    try:
        questions = faq_container.query_selector_all("p.accordio")
    except Exception:
        questions = []
    try:
        answers = faq_container.query_selector_all("div.liv")
    except Exception:
        answers = []

    total = max(len(questions), len(answers))
    for idx in range(total):
        q_text = ""
        ans_text = ""

        if idx < len(questions):
            q = questions[idx]
            try:
                q_text = " ".join(q.inner_text().split()).strip()
            except Exception:
                q_text = ""

            # Expand accordion to reveal answer.
            try:
                page.evaluate("(el) => el.click()", q)
                page.wait_for_timeout(180)
            except Exception:
                pass

        # Prefer indexed answer block; fallback to immediate sibling.
        if idx < len(answers):
            try:
                ans_text = " ".join(answers[idx].inner_text().split()).strip()
            except Exception:
                ans_text = ""
        elif idx < len(questions):
            try:
                ans = questions[idx].evaluate_handle("el => el.nextElementSibling")
                ans_class = (ans.get_attribute("class") or "").lower()
                if "liv" in ans_class:
                    ans_text = " ".join(ans.inner_text().split()).strip()
            except Exception:
                ans_text = ""

        if q_text:
            content.append({"type": "text", "value": q_text})
        if ans_text:
            content.append({"type": "text", "value": ans_text})

    return content


def scrape_single_course_detail(page):
    detail = {
        "url": page.url,
        "about": [],
        "toc_sections": [],
    }

    if _is_security_locked(page):
        detail["skipped_reason"] = "security_lock"
        return detail

    _expand_course_read_more(page)

    article = page.locator("#listing-article")
    wrapper = page.locator(".cdcms_courses")

    if wrapper.count() > 0:
        root = wrapper.first
    elif article.count() > 0:
        root = article.first
    else:
        root = page.locator("main").first
        if not root:
            detail["skipped_reason"] = "article_not_found"
            return detail

    # ==========================
    # ABOUT (STOP AT TOC)
    # ==========================
    nodes = root.locator(":scope > *").element_handles()

    for node in nodes:
        try:
            txt = " ".join(node.inner_text().split()).lower()
        except:
            continue

        if "table of content" in txt or "table of contents" in txt:
            break

        content = _extract_typed_content(node)
        if content:
            detail["about"].extend(content)

    # ==========================
    # TOC (Nested-safe)
    # ==========================
    link_titles = {}
    toc_links = root.locator("ol a[href*='#'], a[href*='#'][data-college_section_name='article']")
    if toc_links.count() == 0:
        toc_links = page.locator("ol a[href*='#'], a[href*='#'][data-college_section_name='article']")
    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        href = (link.get_attribute("href") or "").strip()
        if "#" not in href:
            continue
        if href.startswith("#"):
            anchor = href[1:].strip()
        else:
            anchor = (urlparse(href).fragment or "").strip()
        if not anchor:
            continue
        if anchor not in link_titles:
            link_titles[anchor] = " ".join(link.inner_text().split()).strip()

    toc_items = []
    seen = set()
    for anchor, title in link_titles.items():
        if not anchor or anchor in seen:
            continue
        seen.add(anchor)
        toc_items.append({"anchor": anchor, "title": title or anchor})

    heading_nodes = root.locator("h2[id], h3[id], h4[id], h5[id]").element_handles()
    for h in heading_nodes:
        try:
            anchor = (h.get_attribute("id") or "").strip()
            title = " ".join(h.inner_text().split()).strip()
        except Exception:
            continue
        if not anchor or anchor in seen:
            continue
        seen.add(anchor)
        toc_items.append({"anchor": anchor, "title": link_titles.get(anchor, title)})

    for item in toc_items:
        anchor = item["anchor"]
        title = item["title"]

        if _is_security_locked(page):
            continue

        target = page.locator(f'[id="{anchor}"]')
        if target.count() == 0:
            continue

        section_content = []

        heading_text = target.first.inner_text().strip()
        if heading_text:
            section_content.append({
                "type": "text",
                "value": heading_text
            })

        # FAQ specific: click accordion items and capture full answers.
        if "faq" in title.lower():
            faq_content = _extract_faq_qa_content(page, target.first)
            if faq_content:
                section_content.extend(faq_content)
                detail["toc_sections"].append({
                    "section": title,
                    "content": section_content
                })
                continue

        el = target.first.evaluate_handle("el => el.nextElementSibling")

        while el:
            try:
                tag = el.evaluate("el => el.tagName.toLowerCase()")
                el_id = el.get_attribute("id")
            except:
                break

            if tag in ["h2", "h3", "h4", "h5"] and el_id and el_id != anchor and section_content:
                break

            chunk = _extract_typed_content(el)
            if chunk:
                section_content.extend(chunk)

            el = el.evaluate_handle("el => el.nextElementSibling")

        if section_content:
            detail["toc_sections"].append({
                "section": title,
                "content": section_content
            })

    return detail


def open_syllabus_page(page, course_url):
    base = (course_url or "").strip().rstrip("/")
    if not base:
        return False
    syllabus_url = base + "/syllabus"
    page.goto(syllabus_url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(900)
    return True


def scrape_syllabus_detail(page):
    detail = {
        "url": page.url,
        "about": [],
        "toc_sections": [],
    }

    if _is_security_locked(page):
        detail["skipped_reason"] = "security_lock"
        return detail

    _expand_course_read_more(page)

    article = page.locator("#listing-article")
    wrapper = page.locator(".cdcms_courses")

    if wrapper.count() > 0:
        root = wrapper.first
    elif article.count() > 0:
        root = article.first
    else:
        root = page.locator("main").first
        if not root:
            detail["skipped_reason"] = "article_not_found"
            return detail

    # ABOUT: only content before Table of Contents
    nodes = root.locator(":scope > *").element_handles()
    for node in nodes:
        try:
            txt = " ".join(node.inner_text().split()).lower()
        except Exception:
            continue

        if "table of content" in txt or "table of contents" in txt:
            break

        chunk = _extract_typed_content(node)
        if chunk:
            detail["about"].extend(chunk)

    # TOC links (includes nested and absolute anchor URLs)
    link_titles = {}
    toc_links = root.locator("ol a[href*='#'], a[href*='#'][data-college_section_name='article']")
    if toc_links.count() == 0:
        toc_links = page.locator("ol a[href*='#'], a[href*='#'][data-college_section_name='article']")

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        href = (link.get_attribute("href") or "").strip()
        if "#" not in href:
            continue

        if href.startswith("#"):
            anchor = href[1:].strip()
        else:
            anchor = (urlparse(href).fragment or "").strip()

        if not anchor:
            continue

        if anchor not in link_titles:
            link_titles[anchor] = " ".join(link.inner_text().split()).strip()

    # Build ordered section list from heading ids so nested sections are not missed.
    toc_items = []
    seen = set()
    heading_nodes = root.locator("h2[id], h3[id], h4[id], h5[id]").element_handles()
    for h in heading_nodes:
        try:
            anchor = (h.get_attribute("id") or "").strip()
            title = " ".join(h.inner_text().split()).strip()
        except Exception:
            continue

        if not anchor or anchor in seen:
            continue
        seen.add(anchor)
        toc_items.append({"anchor": anchor, "title": link_titles.get(anchor, title)})

    if not toc_items:
        for anchor, title in link_titles.items():
            toc_items.append({"anchor": anchor, "title": title or anchor})

    for item in toc_items:
        anchor = item["anchor"]
        title = item["title"]

        if _is_security_locked(page):
            continue

        target = page.locator(f'[id="{anchor}"]')
        if target.count() == 0:
            continue

        section_content = []
        heading_text = " ".join(target.first.inner_text().split()).strip()
        if heading_text:
            section_content.append({"type": "text", "value": heading_text})

        el = target.first.evaluate_handle("el => el.nextElementSibling")
        while el:
            try:
                tag = el.evaluate("el => el.tagName.toLowerCase()")
                el_id = (el.get_attribute("id") or "").strip()
            except Exception:
                break

            # stop at next heading boundary
            if tag in ["h2", "h3", "h4", "h5"] and el_id and el_id != anchor and section_content:
                break

            chunk = _extract_typed_content(el)
            if chunk:
                section_content.extend(chunk)

            el = el.evaluate_handle("el => el.nextElementSibling")

        if section_content:
            detail["toc_sections"].append({
                "section": title,
                "content": section_content
            })

    return detail


def _build_course_document(stream_data, course_payload):
    course_meta = {
        "course_name": course_payload.get("course_name", ""),
        "course_url": course_payload.get("course_url", ""),
        "duration": course_payload.get("duration", ""),
        "mode": course_payload.get("mode", ""),
    }

    document = {
        "stream": stream_data.get("stream", ""),
        "stream_name": stream_data.get("stream_name", ""),
        "source_url": (
            stream_data.get("source_url")
            or stream_data.get("all_courses_url")
            or stream_data.get("stream_url")
            or COURSES_URL
        ),
        "stream_url": stream_data.get("stream_url", ""),
        "all_courses_url": stream_data.get("all_courses_url", ""),
        "course_name": course_meta["course_name"],
        "course_url": course_meta["course_url"],
        "course": course_meta,
        "course_index": course_payload.get("course_index"),
        "course_detail": course_payload.get("course_detail", {}),
        "syllabus_detail": course_payload.get("syllabus_detail", {}),
        "final_url": (
            course_payload.get("final_url")
            or course_meta["course_url"]
            or stream_data.get("final_url", "")
        ),
    }

    if course_payload.get("skipped_reason"):
        document["skipped_reason"] = course_payload["skipped_reason"]

    return document


def _course_match_filter(course_doc):
    variants = []
    course_url = course_doc.get("course_url", "")
    course_name = course_doc.get("course_name", "")

    if course_url:
        variants.append({"course_url": course_url})
        variants.append({"course.course_url": course_url})

    if course_name:
        variants.append({"course_name": course_name})
        variants.append({"course.course_name": course_name})

    if variants:
        return {
            "stream": course_doc.get("stream", ""),
            "$or": variants,
        }

    return {
        "stream": course_doc.get("stream", ""),
        "source_url": course_doc.get("source_url", ""),
        "course_index": course_doc.get("course_index"),
    }


def _upsert_course_document(coll, course_doc):
    matches = list(coll.find(_course_match_filter(course_doc), {"_id": 1}))

    if matches:
        course_doc["_id"] = matches[0]["_id"]
        coll.replace_one({"_id": course_doc["_id"]}, course_doc, upsert=True)

        stale_ids = [item["_id"] for item in matches[1:]]
        if stale_ids:
            coll.delete_many({"_id": {"$in": stale_ids}})
    else:
        coll.insert_one(course_doc)


def save_to_mongo(stream_data):
    client = MongoClient(MONGO_URI)
    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]
        result = {
            "saved": 0,
            "cleaned": 0,
            "errors": [],
        }

        result["cleaned"] = coll.delete_many(
            {
                "stream": stream_data.get("stream"),
                "courses": {"$exists": True},
            }
        ).deleted_count

        for course_payload in stream_data.get("courses", []):
            course_doc = _build_course_document(stream_data, course_payload)
            if not course_doc.get("course_name"):
                continue

            try:
                _upsert_course_document(coll, course_doc)
                result["saved"] += 1
            except Exception as exc:
                result["errors"].append(
                    {
                        "course_name": course_doc.get("course_name", ""),
                        "course_url": course_doc.get("course_url", ""),
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )

        return result
    finally:
        client.close()


def main():
    global COURSES_URL, OUTPUT_FILE

    args = _parse_args()
    COURSES_URL = _resolve_runtime_url(args.url)
    OUTPUT_FILE = _resolve_output_file(args.output_file)

    data = {
        "stream": "all_streams",
        "source_url": COURSES_URL,
        "streams": [],
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless, args=BROWSER_ARGS)
        context = browser.new_context(
            user_agent=DEFAULT_USER_AGENT,
            viewport={"width": 1440, "height": 1000},
            locale="en-US",
        )
        context.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        )
        page = context.new_page()

        try:
            if _is_root_courses_url(COURSES_URL):
                stream_metas = _limit_items(_collect_all_streams(page), args.stream_limit)
            else:
                stream_meta = _build_stream_meta_from_url(COURSES_URL)
                if not stream_meta:
                    raise ValueError(
                        "Expected a Collegedunia /courses URL or a direct /courses/<stream> URL."
                    )
                stream_metas = [stream_meta]

            data["stream_total"] = len(stream_metas)

            for index, stream_meta in enumerate(stream_metas, start=1):
                print(
                    f"[stream {index}/{len(stream_metas)}] {stream_meta.get('stream_name', 'unknown stream')}",
                    flush=True,
                )
                try:
                    stream_data = _scrape_stream_payload(
                        page,
                        stream_meta,
                        course_limit=args.course_limit,
                    )
                except Exception as exc:
                    stream_data = {
                        "stream": stream_meta.get("stream", ""),
                        "stream_name": stream_meta.get("stream_name", ""),
                        "source_url": stream_meta.get("stream_url", "") or COURSES_URL,
                        "stream_url": stream_meta.get("stream_url", ""),
                        "all_courses_url": stream_meta.get("all_courses_url", ""),
                        "courses": [],
                        "skipped_reason": f"stream_error: {type(exc).__name__}",
                    }

                try:
                    mongo_result = save_to_mongo(stream_data)
                    stream_data["mongo_status"] = (
                        f"saved_courses={mongo_result['saved']}, "
                        f"cleaned_stream_docs={mongo_result['cleaned']}"
                    )
                    if mongo_result.get("errors"):
                        stream_data["mongo_status"] += (
                            f", failed_courses={len(mongo_result['errors'])}"
                        )
                        stream_data["mongo_errors"] = mongo_result["errors"]
                except Exception as exc:
                    stream_data["mongo_status"] = f"save_error: {type(exc).__name__}: {exc}"

                data["streams"].append(stream_data)
                _write_output_snapshot(data)

            data["final_url"] = page.url
        finally:
            browser.close()

    data["stream_count"] = len(data.get("streams", []))
    data["course_count"] = sum(len(item.get("courses", [])) for item in data.get("streams", []))
    _write_output_snapshot(data)

    print(
        f"Saved {data['stream_count']} streams and {data['course_count']} courses to {OUTPUT_FILE}"
    )
    print(f"Per-course data upsert attempted in MongoDB collection: {MONGO_COLLECTION}")


if __name__ == "__main__":
    main()
