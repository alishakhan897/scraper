from playwright.sync_api import sync_playwright
import re
import json
import time
import sys
import argparse
import os
from pymongo import MongoClient

try:
    # Avoid Windows cp1252 print crashes from Unicode text in logs.
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except:
    pass

DEFAULT_URL = "https://collegedunia.com/university/25455-iit-delhi-indian-institute-of-technology-iitd-new-delhi"
DEFAULT_OUTPUT_FILE = "iim_knp_full_dump.json"
MONGO_URI = "mongodb+srv://alishakhan8488_db_user:DaVHn9goL8STNzNs@cluster0.nkmbpqt.mongodb.net/studentcap?retryWrites=true&w=majority"
MONGO_DB = "studentcap"
MONGO_COLLECTION = "new_college"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Scrape a Collegedunia college or university profile."
    )
    parser.add_argument(
        "--url",
        default="",
        help="Override the default college/university URL.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Write scraped JSON to this path instead of the default output file.",
    )
    parser.add_argument(
        "--update-existing-placements",
        action="store_true",
        help="Update placement data for existing MongoDB colleges instead of scraping one URL.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit colleges when using --update-existing-placements.",
    )
    return parser.parse_args()


def _resolve_runtime_url(cli_url=""):
    return (cli_url or os.getenv("SCRAPER_COLLEGE_URL") or DEFAULT_URL).strip()


def _resolve_output_file(cli_output_file=""):
    return (
        cli_output_file
        or os.getenv("SCRAPER_COLLEGE_OUTPUT_FILE")
        or DEFAULT_OUTPUT_FILE
    ).strip()

# ---------------- UTILITIES ----------------
def safe_goto(page, url, retries=3):
    for i in range(retries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            return True
        except Exception as e:
            print(f"Retry {i+1} for:", url)
            time.sleep(3)

    print("Failed to load:", url)
    return False

def extract_college_id(url: str) -> int:
    m = re.search(r"/(?:university|college)/(\d+)", url)
    if not m:
        raise ValueError("College ID not found in URL")
    return int(m.group(1))

def split_location(text: str):
    parts = [p.strip() for p in text.split(",")]
    return {
        "city": parts[0] if len(parts) > 0 else "",
        "state": parts[1] if len(parts) > 1 else "",
    }

def open_admission_tab(page):
    current_url = page.url.rstrip("/")

    # already on admission
    if current_url.endswith("/admission"):
        return True

    admission_url = current_url + "/admission"

    print("Ã¢Å¾Â¡Ã¯Â¸Â Opening Admission page:", admission_url)
    page.goto(admission_url, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_selector("#listing-article", timeout=30000)
    time.sleep(2)
    return True

def expand_read_more(page):
    # multiple possible selectors
    selectors = [
        "span:has-text('Read More')",
        "a:has-text('Read More')",
        "[data-csm-title='Read More']",
        ".college-page-read-more"
    ]

    for sel in selectors:
        btn = page.locator(sel)
        if btn.count() > 0:
            try:
                btn.first.scroll_into_view_if_needed()
                page.evaluate("(el) => el.click()", btn.first)
                time.sleep(3)
                return True
            except:
                continue
    return False

def _collect_toc_links_from_best_container(page):
    """
    Pick one TOC container (best = max unique anchors) and return all nested links.
    This avoids mixing duplicate desktop/mobile TOCs and supports double-nested TOC items.
    """
    best_links = []
    best_count = 0

    containers = page.locator("#listing-article .cdcms_section1 div:has(a[href^='#'])")
    for i in range(containers.count()):
        c = containers.nth(i)
        try:
            txt = " ".join(c.inner_text().split()).lower()
        except:
            continue

        if "table of content" not in txt and "table of contents" not in txt:
            continue

        links = []
        seen_local = set()
        nodes = c.locator("a[href^='#']")
        for j in range(nodes.count()):
            a = nodes.nth(j)
            try:
                href = (a.get_attribute("href") or "").strip()
                title = " ".join(a.inner_text().split()).strip()
            except:
                continue

            if not href.startswith("#"):
                continue

            anchor = href[1:].strip()
            if not anchor or anchor in seen_local:
                continue

            seen_local.add(anchor)
            links.append({
                "title": title,
                "anchor": anchor
            })

        if len(links) > best_count:
            best_count = len(links)
            best_links = links

    # Fallback: if TOC wrapper not detected, gather all hash links and dedupe by anchor.
    if not best_links:
        nodes = page.locator("#listing-article .cdcms_section1 a[href^='#']")
        seen = set()
        for i in range(nodes.count()):
            a = nodes.nth(i)
            try:
                href = (a.get_attribute("href") or "").strip()
                title = " ".join(a.inner_text().split()).strip()
            except:
                continue
            anchor = href[1:].strip() if href.startswith("#") else ""
            if not anchor or anchor in seen:
                continue
            seen.add(anchor)
            best_links.append({
                "title": title,
                "anchor": anchor
            })

    return best_links

def scrape_toc_by_clicking(page):
    data = []
    toc_links = _collect_toc_links_from_best_container(page)

    for item in toc_links:
        title = item.get("title", "").strip()
        anchor = item.get("anchor", "").strip()
        if not anchor:
            continue

        target = page.locator(f'[id="{anchor}"]')
        if target.count() == 0:
            target = page.locator(f'[name="{anchor}"]')
        if target.count() == 0:
            continue

        heading = target.first.evaluate_handle(
            """el => {
                let n = el;
                while (n && (!n.tagName || !['h2','h3'].includes(n.tagName.toLowerCase()))) {
                    n = n.nextElementSibling;
                }
                return n;
            }"""
        )

        try:
            heading_tag = heading.evaluate("el => el && el.tagName ? el.tagName.toLowerCase() : null")
        except:
            heading_tag = None

        if heading_tag not in ["h2", "h3"]:
            continue

        content = extract_section_by_heading(heading)
        if not content:
            continue

        section_title = title
        if not section_title:
            try:
                section_title = " ".join(heading.inner_text().split()).strip()
            except:
                section_title = anchor

        data.append({
            "section": section_title,
            "content": content
        })

    return data
# ---------------- BASIC HEADER ----------------
def scrape_basic_header(page, data):
    title = page.locator("h1")
    if title.count():
        data["name"] = title.first.inner_text().split(":")[0].strip()

    logo = page.locator(".clg-logo-container img")
    if logo.count():
        data["logo"] = logo.first.get_attribute("src")

    spans = page.locator("div.college_header_details span")
    for i in range(spans.count()):
        txt = spans.nth(i).inner_text().strip()
        if "," in txt and "Estd" not in txt:
            loc = split_location(txt)
            data["city"] = loc["city"]
            data["state"] = loc["state"]
        if "University" in txt or "Institute" in txt:
            data["college_type"] = txt
        if "Estd" in txt:
            yr = re.search(r"\d{4}", txt)
            if yr:
                data["established_year"] = yr.group(0)

    # Header rating/reviews
    try:
        rating_text = ""
        rating_locators = [
            "header div[class*='rating'] div[class*='fs-30']",
            "div[class*='right-section'] div[class*='rating'] div[class*='fs-30']",
            "div[class*='rating'] div[class*='font-weight-bold']",
        ]
        for sel in rating_locators:
            loc = page.locator(sel)
            if loc.count() > 0:
                rating_text = loc.first.inner_text().strip()
                if rating_text:
                    break
        m = re.search(r"\d+(?:\.\d+)?", rating_text or "")
        if m:
            data["rating"] = float(m.group(0))
    except Exception:
        pass

    try:
        reviews_text = ""
        review_locators = [
            "header div[class*='rating'] a:has-text('Reviews')",
            "div[class*='rating'] a:has-text('Reviews')",
            "a[href*='/review']:has-text('Reviews')",
        ]
        for sel in review_locators:
            loc = page.locator(sel)
            if loc.count() > 0:
                reviews_text = loc.first.inner_text().strip()
                if reviews_text:
                    break
        m = re.search(r"(\d+)\s*Reviews?", reviews_text or "", flags=re.IGNORECASE)
        if m:
            data["reviews"] = int(m.group(1))
    except Exception:
        pass

# ---------------- SAFE SECTION EXTRACTOR ----------------
def extract_section_by_heading(heading):
    content = []

    def parse_element(el):
        try:
            tag = el.evaluate("el => el.tagName ? el.tagName.toLowerCase() : null")
        except:
            return

        if not tag:
            return

        # ---------- IMAGE ----------
        if tag == "img":
            src = el.get_attribute("data-src") or el.get_attribute("src")
            if src:
                content.append({
                    "type": "image",
                    "src": src
                })
            return

        # ---------- VIDEO ----------
        if tag in ["iframe", "video"]:
            src = (
                el.get_attribute("src")
                or el.get_attribute("data-src")
                or el.get_attribute("data-lazy-src")
            )
            if not src and tag == "video":
                try:
                    src = el.query_selector("source").get_attribute("src")
                except:
                    src = None
            if src:
                content.append({
                    "type": "video",
                    "src": src
                })
            return

        # ---------- SUB-HEADING ----------
        if tag in ["h3", "h4", "h5"]:
            txt = " ".join(el.inner_text().split()).strip()
            if txt:
                item = {
                    "type": "heading",
                    "level": tag,
                    "value": txt
                }
                try:
                    html = el.inner_html().strip()
                    if html:
                        item["value_html"] = html
                except:
                    pass
                content.append(item)
            return

        # ---------- TEXT ----------
        elif tag == "p":
            for img in el.query_selector_all("img"):
                src = (
                    img.get_attribute("data-src")
                    or img.get_attribute("data-lazy-src")
                    or img.get_attribute("src")
                )
                if src:
                    content.append({
                        "type": "image",
                        "src": src
                    })

            for vid in el.query_selector_all("iframe, video"):
                src = (
                    vid.get_attribute("src")
                    or vid.get_attribute("data-src")
                    or vid.get_attribute("data-lazy-src")
                )
                if src:
                    content.append({
                        "type": "video",
                        "src": src
                    })

            txt = el.inner_text().strip()

            if txt:
                item = {
                    "type": "text",
                    "value": " ".join(txt.split())
                }
                try:
                    if el.query_selector("strong, b"):
                        html = el.inner_html().strip()
                        if html:
                            item["value_html"] = html
                except:
                    pass
                content.append(item)

        # ---------- LIST ----------
        elif tag in ["ul", "ol"]:
            items = []
            for li in el.query_selector_all("li"):
                t = li.inner_text().strip()
                if t:
                    items.append(" ".join(t.split()))
            if items:
                content.append({
                    "type": "list",
                    "value": items
                })

        # ---------- TABLE ----------
        elif tag == "table":
            rows = []
            for tr in el.query_selector_all("tr"):
                cols = [
                    " ".join(td.inner_text().split())
                    for td in tr.query_selector_all("th, td")
                ]
                if cols:
                    rows.append(cols)
            if rows:
                content.append({
                    "type": "table",
                    "value": rows
                })

        # ---------- DIV / SECTION (recursive) ----------
        elif tag in ["div", "section"]:
            for child in el.query_selector_all(":scope > *"):
                parse_element(child)

    # ===== Traverse siblings until next H2 =====
    el = heading.evaluate_handle("el => el.nextElementSibling")

    while el:
        try:
            tag = el.evaluate("el => el.tagName.toLowerCase()")
        except:
            break

        if tag in ["h2"]:
            break

        parse_element(el)
        el = el.evaluate_handle("el => el.nextElementSibling")

    return content

# ---------------- ABOUT + TOC ----------------
def scrape_about_and_toc(page, data):
    page.wait_for_selector("#listing-article", timeout=20000)
    article = page.locator("#listing-article")
    article.scroll_into_view_if_needed()
    time.sleep(1)

    # click Read More
    try:
        page.locator("span:has-text('Read More')").first.click(timeout=3000)
        time.sleep(2)
    except:
        pass

    section = article.locator(".cdcms_section1")

    # -------- ABOUT --------
    # -------- ABOUT + ABOUT HIGHLIGHTS (FIXED) --------
    about_text = []
    about_highlights = []

    nodes = section.locator(":scope > *")
    stage = "about"

    for i in range(nodes.count()):
        node = nodes.nth(i)

        try:
            tag = node.evaluate("el => el.tagName.toLowerCase()")
        except:
            continue

        # -------- ABOUT TEXT (first paragraphs only) --------
        if stage == "about" and tag == "p":
            txt = node.inner_text().strip()
            if len(txt) > 40:
                about_text.append(" ".join(txt.split()))
                continue
            else:
                stage = "highlights"

            # -------- ABOUT HIGHLIGHTS (table / list / text) --------
        if stage == "highlights":
            tables = node.locator("table")
            if tables.count() > 0:
                for t in range(tables.count()):
                    table = tables.nth(t)
                    rows = []
                    for tr in table.locator("tr").all():
                        cols = [c.inner_text().strip() for c in tr.locator("th, td").all()]
                        if cols:
                            rows.append(cols)
                    if rows:
                        about_highlights.append({
                            "type": "table",
                            "value": rows
                        })

    if about_text:
        data["about"] = {
            "format": "text",
            "value": " ".join(about_text)
        }

    if about_highlights and about_highlights:
        data["about_highlights"] = about_highlights

    
    # -------- TOC FIXED --------
    toc_data = []
    headings = section.locator("h2")

    for i in range(headings.count()):
        h = headings.nth(i)
        title = h.inner_text().strip()
        content = extract_section_by_heading(h)

        if content:
            toc_data.append({
                "section": title,
                "content": content
            })

    if toc_data:
        data["toc_sections"] = toc_data

def open_reviews_tab(page):
    base_url = re.sub(r"/(admission|reviews).*", "", page.url.rstrip("/"))
    reviews_url = base_url + "/reviews"

    print("Ã¢Å¾Â¡Ã¯Â¸Â Opening Reviews page:", reviews_url)

    if not safe_goto(page, reviews_url):
        return False
    # wait for reviews container
    page.wait_for_selector("section.like-dislike-section", timeout=30000)
    return True


def scrape_what_students_say(page):
    result = {
        "likes": [],
        "dislikes": []
    }

    # force lazy load
    page.mouse.wheel(0, 1200)
    page.wait_for_timeout(1500)

    section = page.locator("section.like-dislike-section")
    if section.count() == 0:
        return result

    # -------- CLICK "+ More" (LIKES ONLY) --------
    try:
        more_btn = section.locator("button:has-text('More')")
        if more_btn.count() > 0:
            more_btn.first.scroll_into_view_if_needed()
            page.evaluate("(el) => el.click()", more_btn.first)
            page.wait_for_timeout(1000)
    except:
        pass

    # -------- LIKES --------
    likes = section.locator("#likes-dislikes ul li")
    for i in range(likes.count()):
        txt = likes.nth(i).inner_text().strip()
        if txt:
            result["likes"].append(" ".join(txt.split()))

    # -------- DISLIKES --------
    dislikes = section.locator("div.dislike-section ul li")
    for i in range(dislikes.count()):
        txt = dislikes.nth(i).inner_text().strip()
        if txt:
            result["dislikes"].append(" ".join(txt.split()))

    # dedupe
    result["likes"] = list(dict.fromkeys(result["likes"]))
    result["dislikes"] = list(dict.fromkeys(result["dislikes"]))

    return result

def scrape_overall_rating(page):
    rating_data = {}

    container = page.locator(".review-rating")
    if container.count() == 0:
        return rating_data

    # Overall Score
    score = container.locator("span").first.inner_text().strip()
    rating_data["score"] = float(score) if score.replace(".", "", 1).isdigit() else None

    # Verified Reviews
    verified = container.locator("text=Verified Reviews")
    if verified.count():
        txt = verified.first.inner_text()
        num = re.search(r"\d+", txt)
        if num:
            rating_data["total_reviews"] = int(num.group())

    # Breakdown 5Ã¢Â­Â 4Ã¢Â­Â etc
    breakdown = {}
    rows = container.locator("div:has(span.icon-review-star)")
    for row in rows.all():
        stars = row.locator("span.icon-review-star").count()
        percent_bar = row.inner_text()
        count = re.search(r"\d+", percent_bar)
        if stars and count:
            breakdown[str(stars)] = int(count.group())

    rating_data["breakdown"] = breakdown

    return rating_data

def scrape_category_ratings(page):
    ratings = {}

    blocks = page.locator("div:has-text('Academic')")
    for block in blocks.all():
        text = block.inner_text()

        matches = re.findall(r"(Academic|Faculty|Infrastructure|Accommodation)\s+(\d\.\d)", text)
        for name, score in matches:
            ratings[name] = float(score)

    return ratings

def scrape_review_images(page):
    images = []

    page.mouse.wheel(0, 6000)
    time.sleep(2)

    imgs = page.locator("section#reviews img")
    for img in imgs.all():
        src = img.get_attribute("src")
        if src and "/reviewPhotos/" in src:
            images.append(src)

    return list(set(images))

def scrape_reviews_page(page):
    data = {}

    data["what_students_say"] = scrape_what_students_say(page)
    data["overall_rating"] = scrape_overall_rating(page)
    data["category_ratings"] = scrape_category_ratings(page)
    data["gallery_images"] = scrape_review_images(page)

    return data

def scrape_all_qna(page, college_id):
    qna_results = []

    qna_url = f"https://collegedunia.com/qna?college={college_id}"
    page.goto(qna_url, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    # all question cards
    question_links = page.locator(
        "h3[data-test-id='question-card-title'] a"
    )

    total_questions = question_links.count()
    print(f"Ã°Å¸Å¸Â¢ Total Questions Found: {total_questions}")

    for i in range(total_questions):
        # re-locate every loop (DOM refresh safe)
        question_links = page.locator(
            "h3[data-test-id='question-card-title'] a"
        )
        link = question_links.nth(i)

        question_href = link.get_attribute("href")
        question_title = link.inner_text().strip()

        print(f"Ã¢Å¾Â¡Ã¯Â¸Â Opening Q{i+1}: {question_title}")

        # ---------------- OPEN QUESTION PAGE ----------------
        page.goto("https://collegedunia.com" + question_href, wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

        qna_obj = {
            "question": "",
            "answers": []
        }

        # ---------------- QUESTION TITLE ----------------
        try:
            qna_obj["question"] = page.locator("h1").first.inner_text().strip()
        except:
            qna_obj["question"] = question_title

        # ---------------- ALL ANSWER CARDS ----------------
        answer_cards = page.locator("div[id^='answer-']")

        for j in range(answer_cards.count()):
            card = answer_cards.nth(j)
            ans = {}

            # ---------- AUTHOR ----------
            try:
                ans["author"] = card.locator(
                    "div.author-info-wrapper span"
                ).first.inner_text().strip()
            except:
                ans["author"] = ""

            # ---------- QUALIFICATION ----------
            try:
                ans["qualification"] = card.locator(
                    "div.text-md.font-weight-bold"
                ).inner_text().strip()
            except:
                ans["qualification"] = ""

            # ---------- POSTED ON ----------
            try:
                ans["posted_on"] = card.locator(
                    "span:has-text('Posted On')"
                ).inner_text().replace("Posted On -", "").replace("Posted On :", "").strip()
            except:
                ans["posted_on"] = ""

            # =================================================
            # Ã°Å¸â€Â¥ CLICK READ MORE (ALL VARIANTS)
            # =================================================
            try:
                read_more = card.locator(
                    "span:has-text('Read More'), span[data-test-id='ques-read-more']"
                )
                if read_more.count() > 0:
                    read_more.first.scroll_into_view_if_needed()
                    page.evaluate("(el) => el.click()", read_more.first)
                    page.wait_for_timeout(1200)
            except:
                pass

            # =================================================
            # Ã°Å¸â€Â¥ UNIVERSAL ANSWER EXTRACTION (FIXED)
            # =================================================
            answer_text = []

            try:
                container = card.locator("div.answer-description")

                if container.count() > 0:
                    raw = container.inner_text()

                    for line in raw.split("\n"):
                        line = " ".join(line.split())
                        if (
                            line
                            and not line.lower().startswith("read")
                            and line.lower() != "share"
                        ):
                            answer_text.append(line)

            except:
                pass

            # only push meaningful answers
            if answer_text:
                ans["answer"] = answer_text
                qna_obj["answers"].append(ans)

        # only push question if it has answers
        if qna_obj["answers"]:
            qna_results.append(qna_obj)

        # ---------------- BACK TO QNA LIST ----------------
        page.go_back(wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

    return qna_results

def scrape_ranking_about(article):
    about = []

    container = article.locator("div.cdcms_ranking")
    nodes = container.locator(":scope > *")

    for i in range(nodes.count()):
        node = nodes.nth(i)

        try:
            tag = node.evaluate("el => el.tagName.toLowerCase()")
        except:
            continue

        # STOP at first H2 Ã¢â€ â€™ rest belongs to TOC
        if tag == "h2":
            break

        # reuse safe parser
        content = []
        if tag:
            content = extract_section_by_heading(node) if tag in ["div", "section"] else []

        # fallback text
        if tag == "p":
            txt = node.inner_text().strip()
            if txt:
                about.append({
                    "type": "text",
                    "value": " ".join(txt.split())
                })

    return about

def scrape_ranking_toc(article):
    toc_sections = []

    # Table of Content links
    toc_links = article.locator(
    "a[data-college_section_name='article'][href^='#']")

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        title = link.inner_text().strip()
        href = link.get_attribute("href") or ""

        anchor = None
        if "#" in href:
            anchor = href.split("#")[-1]

        # click TOC item (scrolls page)
        try:
            link.click()
            time.sleep(1.2)
        except:
            pass

        section_content = []

        # =================================================
        # CASE 1: NORMAL ANCHOR BASED SECTIONS
        # =================================================
        if anchor:
            target = article.locator(f'[id="{anchor}"]')
            if target.count() > 0:
                el = target.first.evaluate_handle("el => el.nextElementSibling")

                while el:
                    try:
                        tag = el.evaluate("el => el.tagName.toLowerCase()")
                    except:
                        break

                    # stop at next main heading
                    if tag == "h2" and section_content:
                        break

                    # SAFE content extraction
                    try:
                        section_content.extend(
                            extract_section_by_heading(el)
                        )
                    except:
                        pass

                    el = el.evaluate_handle("el => el.nextElementSibling")

        # =================================================
        # CASE 2: FAQ SECTION (NO H2, NO ANCHOR RELIANCE)
        # =================================================
        if not section_content and "faq" in title.lower():
            # each FAQ is wrapped in a single block
            faq_blocks = article.locator("div:has(strong:has-text('Ques'))")

            for block in faq_blocks.all():
                try:
                    q = block.locator("strong").first.inner_text().strip()

                    full_text = block.inner_text().strip()
                    ans = full_text.replace(q, "").strip()

                    if q and ans:
                        section_content.append({
                            "type": "qa",
                            "question": q,
                            "answer": ans
                        })
                except:
                    continue

        # =================================================
        # PUSH SECTION
        # =================================================
        if section_content:
            toc_sections.append({
                "section": title,
                "content": section_content
            })

    return toc_sections


def scrape_ranking_page(page):
    ranking_data = {}

    base_url = re.sub(r"/(admission|reviews|ranking|qna).*", "", page.url.rstrip("/"))
    ranking_url = base_url + "/ranking"

    print("Ã°Å¸â€œÅ  Opening Ranking page:", ranking_url)
    page.goto(ranking_url, wait_until="domcontentloaded")
    page.wait_for_selector("#listing-article", timeout=30000)
    time.sleep(1)

    article = page.locator("#listing-article")

    # READ MORE
    try:
        page.locator("span:has-text('Read More')").first.click()
        time.sleep(1.5)
    except:
        pass

    # ---------- ABOUT (STRICT) ----------
    about = scrape_ranking_about(article)
    if about:
        ranking_data["about"] = about

    # ---------- TOC (ANCHOR BASED) ----------
    toc_sections = scrape_ranking_toc(article)
    if toc_sections:
        ranking_data["toc_sections"] = toc_sections

    return ranking_data

PACKAGE_VALUE_PATTERN = re.compile(
    r"(?:INR|Rs\.?|₹)?\s*\d+(?:\.\d+)?\s*(?:CPA|Cr|Crore(?:s)?|LPA|Lakh(?:s)?|Lac(?:s)?|L)\b",
    re.IGNORECASE
)

def _normalize_package_value(raw_text):
    text = " ".join((raw_text or "").split()).strip(" ,:;-")
    if not text:
        return ""

    match = PACKAGE_VALUE_PATTERN.search(text)
    if not match:
        return ""

    value = " ".join(match.group(0).split())
    value = re.sub(r"^(?:Rs\.?|₹)\s*", "INR ", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcpa\b", "CPA", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcr\b", "Cr", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcrore\b", "Crore", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcrores\b", "Crores", value, flags=re.IGNORECASE)
    value = re.sub(r"\blpa\b", "LPA", value, flags=re.IGNORECASE)
    value = re.sub(r"\blakhs\b", "Lakhs", value, flags=re.IGNORECASE)
    value = re.sub(r"\blakh\b", "Lakh", value, flags=re.IGNORECASE)
    value = re.sub(r"\blacs\b", "Lacs", value, flags=re.IGNORECASE)
    value = re.sub(r"\blac\b", "Lac", value, flags=re.IGNORECASE)
    value = re.sub(r"(?<![A-Za-z])l(?![A-Za-z])", "L", value, flags=re.IGNORECASE)
    return value

def _extract_package_candidates(texts):
    values = []
    seen = set()

    for text in texts or []:
        if not text:
            continue

        for match in PACKAGE_VALUE_PATTERN.finditer(text):
            normalized = _normalize_package_value(match.group(0))
            if not normalized:
                continue

            key = normalized.lower()
            if key in seen:
                continue

            seen.add(key)
            values.append(normalized)

    return values

def _extract_labeled_package_value(text, label):
    if not text:
        return ""

    pattern = re.compile(
        rf"{label}.{{0,120}}?((?:INR|Rs\.?|₹)?\s*\d+(?:\.\d+)?\s*(?:CPA|Cr|Crore(?:s)?|LPA|Lakh(?:s)?|Lac(?:s)?|L))",
        re.IGNORECASE
    )
    match = pattern.search(text)
    return _normalize_package_value(match.group(1)) if match else ""

def _extract_package_value_from_labels(text, labels):
    for label in labels:
        value = _extract_labeled_package_value(text, label)
        if value:
            return value
    return ""

def _extract_named_section_from_text(text, start_markers, end_markers):
    normalized_text = " ".join((text or "").split())
    if not normalized_text:
        return ""

    lowered = normalized_text.lower()
    start_index = -1
    matched_marker = ""

    for marker in start_markers:
        idx = lowered.find(marker.lower())
        if idx != -1 and (start_index == -1 or idx < start_index):
            start_index = idx
            matched_marker = marker

    if start_index == -1:
        return ""

    search_from = start_index + len(matched_marker)
    end_index = len(normalized_text)

    for marker in end_markers:
        idx = lowered.find(marker.lower(), search_from)
        if idx != -1 and idx < end_index:
            end_index = idx

    return normalized_text[start_index:end_index].strip()

def _extract_overall_placement_section_texts(article):
    try:
        return article.evaluate(
            """(root) => {
                const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
                const headings = Array.from(root.querySelectorAll("h2, h3"));
                const heading = headings.find((el) =>
                    /overall placement highlights/i.test(normalize(el.innerText))
                );

                if (!heading) {
                    return [];
                }

                const parts = [];
                let node = heading.nextElementSibling;
                while (node) {
                    const tag = (node.tagName || "").toLowerCase();
                    if (tag === "h2" || tag === "h3") {
                        break;
                    }

                    const text = normalize(node.innerText);
                    if (text) {
                        parts.push(text);
                    }

                    node = node.nextElementSibling;
                }

                return parts;
            }"""
        )
    except:
        return []

def _extract_package_texts_from_label_container(article):
    try:
        return article.evaluate(
            """(root) => {
                const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
                const hasBothLabels = (text) =>
                    text.includes("Highest Package") && text.includes("Average Package");

                const candidates = Array.from(
                    root.querySelectorAll("div, section, article")
                ).filter((el) => hasBothLabels(normalize(el.innerText)));

                let best = null;
                for (const el of candidates) {
                    const childHasBoth = Array.from(el.children || []).some((child) =>
                        hasBothLabels(normalize(child.innerText))
                    );
                    if (!childHasBoth) {
                        best = el;
                        break;
                    }
                }

                if (!best && candidates.length) {
                    best = candidates[0];
                }

                if (!best) {
                    return [];
                }

                return Array.from(best.querySelectorAll("span, p, strong, b, div"))
                    .map((el) => normalize(el.innerText))
                    .filter((text) => text && text.length <= 80);
            }"""
        )
    except:
        return []

def scrape_placement_package_highlights(article):
    try:
        article_text = " ".join(article.inner_text().split())
    except:
        article_text = ""

    section_end_markers = [
        "coursewise placement highlights",
        "company-wise placement",
        "placement top recruiters",
        "top recruiters",
        "placement faqs",
        "students' opinion",
        "students opinion",
        "placement experience",
        "course finder",
    ]

    preferred_section_text = _extract_named_section_from_text(
        article_text,
        ["overall placement highlights"],
        section_end_markers
    )

    if not preferred_section_text:
        preferred_section_text = _extract_named_section_from_text(
            article_text,
            ["placement 2025 - yearly trends", "placement yearly trends", "yearly trends"],
            section_end_markers + ["comparison with other iits", "comparison with other colleges"]
        )

    if preferred_section_text:
        highest_package = _extract_package_value_from_labels(
            preferred_section_text,
            [
                "highest package",
                "highest international package",
                "highest domestic package",
                "highest salary package",
                "international package",
                "domestic package",
            ]
        )
        average_package = _extract_package_value_from_labels(
            preferred_section_text,
            [
                "average package",
                "average placement package",
                "average salary package",
            ]
        )

        package_highlights = {}
        if highest_package:
            package_highlights["highest_package"] = highest_package
        if average_package:
            package_highlights["average_package"] = average_package
        if package_highlights:
            return package_highlights

        overall_values = _extract_package_candidates([preferred_section_text])
        if len(overall_values) >= 2:
            return {
                "highest_package": overall_values[0],
                "average_package": overall_values[1]
            }

    overall_section_texts = _extract_overall_placement_section_texts(article)
    if overall_section_texts:
        overall_values = _extract_package_candidates(overall_section_texts)
        if len(overall_values) >= 2:
            return {
                "highest_package": overall_values[0],
                "average_package": overall_values[1]
            }

    highest_package = _extract_package_value_from_labels(
        article_text,
        [
            "highest package",
            "highest international package",
            "highest domestic package",
            "highest salary package",
            "international package",
            "domestic package",
        ]
    )
    average_package = _extract_package_value_from_labels(
        article_text,
        [
            "average package",
            "average placement package",
            "average salary package",
        ]
    )

    package_highlights = {}
    if highest_package:
        package_highlights["highest_package"] = highest_package
    if average_package:
        package_highlights["average_package"] = average_package
    if package_highlights:
        return package_highlights

    raw_texts = _extract_package_texts_from_label_container(article)

    values = _extract_package_candidates(raw_texts)
    if len(values) >= 2:
        return {
            "highest_package": values[0],
            "average_package": values[1]
        }

    return package_highlights

def open_placement_tab(page):
    base_url = re.sub(r"/(admission|reviews|ranking|placement|qna).*", "", page.url.rstrip("/"))
    placement_url = base_url + "/placement"

    print("Ã¢Å¾Â¡Ã¯Â¸Â Opening Placement page:", placement_url)
    page.goto(placement_url, wait_until="domcontentloaded")
    page.wait_for_selector("#listing-article", timeout=30000)
    time.sleep(1)

def _load_lazy_placement_sections(page, scroll_steps=8, scroll_amount=1400):
    for _ in range(scroll_steps):
        try:
            page.mouse.wheel(0, scroll_amount)
            page.wait_for_timeout(600)
        except:
            break

def scrape_placement_about(article):
    about = []

    section = article.locator("div.cdcms_section1")
    nodes = section.locator(":scope > *")

    for i in range(nodes.count()):
        node = nodes.nth(i)

        try:
            tag = node.evaluate("el => el.tagName.toLowerCase()")
        except:
            continue

        # Ã¢â€ºâ€ STOP as soon as first H2 appears
        if tag == "h2":
            break

        # Ã¢Å“â€¦ ONLY top-level intro content
        if tag == "p":
            txt = node.inner_text().strip()
            if txt and len(txt) > 40:
                about.append({
                    "type": "text",
                    "value": " ".join(txt.split())
                })

        elif tag == "table":
            rows = []
            for tr in node.locator("tr").all():
                cols = [c.inner_text().strip() for c in tr.locator("th, td").all()]
                if cols:
                    rows.append(cols)

            if rows:
                about.append({
                    "type": "table",
                    "value": rows
                })

    return about

def scrape_placement_toc(article):
    toc_sections = []

    # Click TOC Read More
    try:
        article.locator(
            "span:has-text('Read More'), a:has-text('Read More')"
        ).first.click()
        time.sleep(1.2)
    except:
        pass

    toc_links = article.locator("div:has-text('Table of Content') ol li a")

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        title = link.inner_text().strip()
        href = link.get_attribute("href") or ""

        anchor = None
        if "#" in href:
            anchor = href.split("#")[-1].strip()

        try:
            link.click()
            time.sleep(1.2)
        except:
            pass

        section_content = []

        # =================================================
        # Ã¢Å“â€¦ SAFE ANCHOR HANDLING (NUMERIC IDS FIXED)
        # =================================================
        if anchor:
            # IMPORTANT FIX Ã°Å¸â€˜â€¡
            target = article.locator(f'[id="{anchor}"]')

            if target.count() > 0:
                el = target.first.evaluate_handle("el => el.nextElementSibling")

                while el:
                    try:
                        tag = el.evaluate("el => el.tagName.toLowerCase()")
                    except:
                        break

                    if tag == "h2" and section_content:
                        break

                    try:
                        section_content.extend(
                            extract_section_by_heading(el)
                        )
                    except:
                        pass

                    el = el.evaluate_handle("el => el.nextElementSibling")

        if section_content:
            toc_sections.append({
                "section": title,
                "content": section_content
            })

    return toc_sections

def scrape_placement_page(page):
    placement_data = {}

    open_placement_tab(page)
    _load_lazy_placement_sections(page)

    article = page.locator("#listing-article")

    # expand top read more
    try:
        page.locator("span:has-text('Read More')").first.click()
        time.sleep(1.2)
    except:
        pass

    # ABOUT
    about = scrape_placement_about(article)
    if about:
        placement_data["about"] = about

    package_highlights = scrape_placement_package_highlights(article)
    if package_highlights:
        placement_data["package_highlights"] = package_highlights

    # TOC
    toc_sections = scrape_placement_toc(article)
    if toc_sections:
        placement_data["toc_sections"] = toc_sections

    return placement_data

def open_cutoff_tab(page):
    base_url = re.sub(r"/(admission|reviews|ranking|placement|faculty|gallery|qna|cutoff).*", "", page.url.rstrip("/"))
    cutoff_url = base_url + "/cutoff"

    print("Opening Cutoff page:", cutoff_url)
    page.goto(cutoff_url, wait_until="domcontentloaded")
    page.wait_for_selector("#listing-article", timeout=30000)
    page.wait_for_timeout(1200)
    return True

def _extract_cutoff_content_from_element(el):
    content = []

    def parse(node):
        try:
            tag = node.evaluate("n => n.tagName ? n.tagName.toLowerCase() : null")
        except:
            return

        if not tag:
            return

        if tag == "img":
            src = node.get_attribute("data-src") or node.get_attribute("src")
            if src:
                content.append({"type": "image", "src": src})
            return

        if tag == "p":
            text = " ".join(node.inner_text().split()).strip()
            if text:
                content.append({"type": "text", "value": text})
            for img in node.query_selector_all("img"):
                src = img.get_attribute("data-src") or img.get_attribute("src")
                if src:
                    content.append({"type": "image", "src": src})
            return

        if tag in ["ul", "ol"]:
            items = []
            for li in node.query_selector_all("li"):
                item = " ".join(li.inner_text().split()).strip()
                if item:
                    items.append(item)
            if items:
                content.append({"type": "list", "value": items})
            return

        if tag == "table":
            rows = []
            for tr in node.query_selector_all("tr"):
                cols = []
                for cell in tr.query_selector_all("th, td"):
                    val = " ".join(cell.inner_text().split()).strip()
                    if val:
                        cols.append(val)
                if cols:
                    rows.append(cols)
            if rows:
                content.append({"type": "table", "value": rows})
            return

        if tag in ["div", "section", "article"]:
            for child in node.query_selector_all(":scope > *"):
                parse(child)
            return

        text = " ".join(node.inner_text().split()).strip()
        if text:
            content.append({"type": "text", "value": text})

    parse(el)
    return content

def _expand_cutoff_read_more(page, article):
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
            btns = article.locator(sel)
            for i in range(btns.count()):
                btn = btns.nth(i)
                try:
                    if not btn.is_visible():
                        continue
                    btn.scroll_into_view_if_needed()
                    page.evaluate("(el) => el.click()", btn)
                    page.wait_for_timeout(450)
                    clicked += 1
                except:
                    continue
        if clicked == 0:
            break

def scrape_cutoff_about(page, article):
    about = []
    _expand_cutoff_read_more(page, article)

    nodes = article.locator(":scope > *").element_handles()
    for node in nodes:
        try:
            tag = node.evaluate("el => el.tagName.toLowerCase()")
            txt = " ".join(node.inner_text().split()).strip().lower()
        except:
            continue

        if tag in ["h2", "h3"] and "table of content" in txt:
            break

        content = _extract_cutoff_content_from_element(node)
        if content:
            about.extend(content)

    return about

def scrape_cutoff_toc(page, article):
    toc_sections = []
    seen_anchors = set()

    _expand_cutoff_read_more(page, article)

    toc_links = article.locator("a[href^='#'][data-college_section_name='article']")
    if toc_links.count() == 0:
        toc_links = article.locator("ol a[href^='#']")

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        title = " ".join(link.inner_text().split()).strip()
        href = (link.get_attribute("href") or "").strip()
        anchor = href[1:] if href.startswith("#") else ""

        if not anchor or anchor in seen_anchors:
            continue
        seen_anchors.add(anchor)

        try:
            link.scroll_into_view_if_needed()
            link.click()
            page.wait_for_timeout(900)
        except:
            pass

        target = article.locator(f'[id="{anchor}"]')
        if target.count() == 0:
            continue

        section_content = []
        el = target.first.evaluate_handle("el => el.nextElementSibling")

        while el:
            try:
                tag = el.evaluate("el => el.tagName.toLowerCase()")
            except:
                break

            if tag == "h2" and section_content:
                break

            chunk = _extract_cutoff_content_from_element(el)
            if chunk:
                section_content.extend(chunk)

            el = el.evaluate_handle("el => el.nextElementSibling")

        if section_content:
            toc_sections.append({
                "section": title or anchor,
                "content": section_content
            })

    return toc_sections

def scrape_cutoff_page(page):
    cutoff_data = {}
    if not open_cutoff_tab(page):
        return cutoff_data

    article = page.locator("#listing-article")

    about = scrape_cutoff_about(page, article)
    if about:
        cutoff_data["about"] = about

    toc_sections = scrape_cutoff_toc(page, article)
    if toc_sections:
        cutoff_data["toc_sections"] = toc_sections

    return cutoff_data

def open_scholarship_tab(page):
    base_url = re.sub(r"/(admission|reviews|ranking|placement|faculty|gallery|qna|cutoff|scholarship).*", "", page.url.rstrip("/"))
    scholarship_url = base_url + "/scholarship"

    print("Opening Scholarship page:", scholarship_url)
    page.goto(scholarship_url, wait_until="domcontentloaded")
    page.wait_for_selector("#listing-article", timeout=30000)
    page.wait_for_timeout(1200)
    return True

def _expand_scholarship_read_more(page, article):
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
            btns = article.locator(sel)
            for i in range(btns.count()):
                btn = btns.nth(i)
                try:
                    if not btn.is_visible():
                        continue
                    btn.scroll_into_view_if_needed()
                    page.evaluate("(el) => el.click()", btn)
                    page.wait_for_timeout(450)
                    clicked += 1
                except:
                    continue
        if clicked == 0:
            break

def scrape_scholarship_about(page, article):
    about = []
    _expand_scholarship_read_more(page, article)

    # Scholarship pages often keep full content inside one wrapper.
    # Parse wrapper's direct children and stop at TOC boundary to avoid repeats.
    root = article
    scholarship_wrapper = article.locator(":scope > .cdcms_scholarships")
    if scholarship_wrapper.count() > 0:
        root = scholarship_wrapper.first

    nodes = root.locator(":scope > *").element_handles()
    for node in nodes:
        try:
            tag = node.evaluate("el => el.tagName.toLowerCase()")
            txt = " ".join(node.inner_text().split()).strip().lower()
        except:
            continue

        # Stop at any TOC marker block (heading / strong label / toc list container).
        if "table of content" in txt:
            break
        try:
            has_toc_strong = node.query_selector("strong") and "table of content" in node.query_selector("strong").inner_text().lower()
        except:
            has_toc_strong = False
        try:
            has_toc_list = node.query_selector("ol a[href^='#']") is not None
        except:
            has_toc_list = False
        if has_toc_strong or has_toc_list:
            break

        content = _extract_cutoff_content_from_element(node)
        if content:
            about.extend(content)

    return about

def scrape_scholarship_toc(page, article):
    toc_sections = []
    seen_anchors = set()

    _expand_scholarship_read_more(page, article)

    toc_links = article.locator("a[href^='#'][data-college_section_name='article']")
    if toc_links.count() == 0:
        toc_links = article.locator("ol a[href^='#']")

    for i in range(toc_links.count()):
        link = toc_links.nth(i)
        title = " ".join(link.inner_text().split()).strip()
        href = (link.get_attribute("href") or "").strip()
        anchor = href[1:] if href.startswith("#") else ""

        if not anchor or anchor in seen_anchors:
            continue
        seen_anchors.add(anchor)

        try:
            link.scroll_into_view_if_needed()
            link.click()
            page.wait_for_timeout(900)
        except:
            pass

        target = article.locator(f'[id="{anchor}"]')
        if target.count() == 0:
            continue

        section_content = []
        el = target.first.evaluate_handle("el => el.nextElementSibling")

        while el:
            try:
                tag = el.evaluate("el => el.tagName.toLowerCase()")
            except:
                break

            if tag == "h2" and section_content:
                break

            chunk = _extract_cutoff_content_from_element(el)
            if chunk:
                section_content.extend(chunk)

            el = el.evaluate_handle("el => el.nextElementSibling")

        if section_content:
            toc_sections.append({
                "section": title or anchor,
                "content": section_content
            })

    return toc_sections

def scrape_scholarship_page(page):
    scholarship_data = {}
    if not open_scholarship_tab(page):
        return scholarship_data

    article = page.locator("#listing-article")

    about = scrape_scholarship_about(page, article)
    if about:
        scholarship_data["about"] = about

    toc_sections = scrape_scholarship_toc(page, article)
    if toc_sections:
        scholarship_data["toc_sections"] = toc_sections

    return scholarship_data

def scrape_gallery_images(page, college_url):
    gallery_images = []

    # 1Ã¯Â¸ÂÃ¢Æ’Â£ Navigate to gallery page
    gallery_url = college_url.rstrip("/") + "/gallery"
    page.goto(gallery_url, timeout=60000)
    page.wait_for_load_state("networkidle")

    # 2Ã¯Â¸ÂÃ¢Æ’Â£ Wait for images container
    page.wait_for_selector("div.img-container img", timeout=15000)

    # 3Ã¯Â¸ÂÃ¢Æ’Â£ Select all images
    images = page.locator("div.img-container img")

    total = images.count()
    print(f"Total images found: {total}")

    # 4Ã¯Â¸ÂÃ¢Æ’Â£ Limit to max 20
    limit = min(total, 20)

    for i in range(limit):
        img = images.nth(i)

        try:
            src = img.get_attribute("src")
            alt = img.get_attribute("alt")

            if src:
                gallery_images.append({
                    "src": src.strip(),
                    "alt": alt.strip() if alt else ""
                })

        except:
            continue

    print(f"Final images collected: {len(gallery_images)}")

    return gallery_images

def open_faculty_tab(page):
    base_url = re.sub(r"/(admission|reviews|ranking|placement|faculty|gallery|qna).*", "", page.url.rstrip("/"))
    faculty_url = base_url + "/faculty"

    print("ðŸ‘¨â€ðŸ« Opening Faculty page:", faculty_url)
    page.goto(faculty_url, wait_until="domcontentloaded")
    page.wait_for_timeout(1200)
    return True

def expand_read_view_more_buttons(page):
    selectors = [
        "button:has-text('Read More')",
        "button:has-text('View More')",
        "button:has-text('More')",
        "a:has-text('Read More')",
        "a:has-text('View More')",
        "a:has-text('More')",
        "span:has-text('Read More')",
        "span:has-text('View More')",
        "span:has-text('More')",
    ]

    clicked = 0
    for sel in selectors:
        loc = page.locator(sel)
        count = loc.count()
        for i in range(count):
            btn = loc.nth(i)
            try:
                if not btn.is_visible():
                    continue
                btn.scroll_into_view_if_needed()
                page.evaluate("(el) => el.click()", btn)
                page.wait_for_timeout(500)
                clicked += 1
            except:
                continue
    return clicked

def scrape_faculty_cards(page):
    faculty = []

    # Keep expanding and scrolling until cards stop increasing.
    stable_rounds = 0
    previous_count = -1

    for _ in range(8):
        expand_read_view_more_buttons(page)
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(800)

        cards = page.locator("div[class*='faculty-card']")
        current_count = cards.count()

        if current_count <= previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            previous_count = current_count

        if stable_rounds >= 2:
            break

    cards = page.locator("div[class*='faculty-card']")

    for i in range(cards.count()):
        card = cards.nth(i)

        name = ""
        designation = ""

        text_blocks = card.locator("div")
        for j in range(text_blocks.count()):
            txt = " ".join(text_blocks.nth(j).inner_text().split()).strip()
            if not txt:
                continue

            if not name and txt.lower() not in ["faculty member", "view more", "read more"]:
                if len(txt) > 2:
                    name = txt
                    continue

            if not designation and txt.lower() != name.lower():
                designation = txt

        if name:
            item = {"name": name}
            if designation and designation.lower() != name.lower():
                item["designation"] = designation
            faculty.append(item)

    # de-duplicate by name
    seen = set()
    cleaned = []
    for f in faculty:
        key = f.get("name", "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            cleaned.append(f)

    return cleaned

def scrape_faculty_page(page):
    faculty_data = {"members": []}

    if open_faculty_tab(page):
        print("ðŸ§‘â€ðŸ« Scraping Faculty page...")
        members = scrape_faculty_cards(page)
        faculty_data["members"] = members

    return faculty_data

def _normalize_text(text):
    return " ".join((text or "").split()).strip()

def _extract_events_rows_from_container(container):
    rows_data = []
    table = container.locator("table")
    if table.count() == 0:
        return rows_data

    rows = table.first.locator("tbody tr")
    for i in range(rows.count()):
        tr = rows.nth(i)
        tds = tr.locator("td")
        if tds.count() < 2:
            continue

        event_name = _normalize_text(tds.nth(0).inner_text())
        date_text = _normalize_text(tds.nth(1).inner_text())

        status = []
        try:
            for s in tds.nth(1).locator("span").all():
                st = _normalize_text(s.inner_text())
                if st:
                    status.append(st)
        except:
            pass

        if event_name or date_text:
            item = {
                "event": event_name,
                "date": date_text
            }
            if status:
                item["status"] = status
            rows_data.append(item)

    return rows_data

def _extract_events_by_heading(section_root, heading_text):
    heading = section_root.locator(f"h2:has-text('{heading_text}')").first
    if heading.count() == 0:
        return []

    container = heading.locator("xpath=following-sibling::div[1]")
    if container.count() == 0:
        return []

    return _extract_events_rows_from_container(container.first)

def _click_all_filter_for_important_dates(section_root, page):
    selectors = [
        "button:has-text('All')",
        "[data-csm-title='All']",
        "[data-ga-title='All']",
    ]

    for sel in selectors:
        btn = section_root.locator(sel)
        if btn.count() == 0:
            continue
        for i in range(btn.count()):
            b = btn.nth(i)
            try:
                if not b.is_visible():
                    continue
                b.scroll_into_view_if_needed()
                page.evaluate("(el) => el.click()", b)
                page.wait_for_timeout(800)
                return True
            except:
                continue
    return False

def _expand_expired_events_show_more(section_root, page, max_clicks=20):
    prev_rows = 0

    for _ in range(max_clicks):

        rows = section_root.locator("tbody tr").count()

        # if rows stop increasing -> stop clicking
        if rows == prev_rows:
            break

        prev_rows = rows

        show_more = section_root.locator("button:has-text('Show More')")

        if show_more.count() == 0:
            break

        clicked = False

        for i in range(show_more.count()):
            btn = show_more.nth(i)

            try:
                if not btn.is_visible():
                    continue

                btn.scroll_into_view_if_needed()
                page.evaluate("(el) => el.click()", btn)

                page.wait_for_timeout(900)
                clicked = True
                break

            except:
                continue

        if not clicked:
            break
def scrape_important_dates(page):
    data = {
        "important_events": [],
        "expired_events": []
    }

    section = page.locator("#application-dates")
    if section.count() == 0:
        return data

    section = section.first
    section.scroll_into_view_if_needed()
    page.wait_for_timeout(500)

    _click_all_filter_for_important_dates(section, page)
    data["important_events"] = _extract_events_by_heading(section, "Important Events")

    _expand_expired_events_show_more(section, page)
    data["expired_events"] = _extract_events_by_heading(section, "Expired Events")

    return data


#def save_to_mongo(data):
    #client = MongoClient(MONGO_URI)
    #try:
        #coll = client[MONGO_DB][MONGO_COLLECTION]
        #coll.replace_one(
            #{"source_college_id": data.get("source_college_id"), "url": data.get("url")},
            #data,
            #upsert=True,
        #)
    #finally:
        #client.close()

def update_mongo_section(college_id, section_name, section_data):
    client = MongoClient(MONGO_URI)

    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]

        coll.update_one(
            {"source_college_id": college_id},
            {
                "$set": {
                    section_name: section_data
                }
            },
            upsert=True
        )

    finally:
        client.close()

def fetch_existing_colleges_for_placement_update(limit=None):
    client = MongoClient(MONGO_URI)

    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]
        cursor = coll.find(
            {
                "source_college_id": {"$exists": True, "$ne": None},
                "url": {
                    "$exists": True,
                    "$type": "string",
                    "$regex": r"^https?://(?:www\.)?collegedunia\.com/",
                    "$options": "i"
                }
            },
            {
                "_id": 0,
                "source_college_id": 1,
                "url": 1,
                "basic.name": 1
            }
        ).sort("source_college_id", 1)

        colleges = list(cursor)
        return colleges[:limit] if limit else colleges
    finally:
        client.close()

def _extract_cli_limit():
    for arg in sys.argv[1:]:
        if not arg.startswith("--limit="):
            continue

        try:
            return int(arg.split("=", 1)[1])
        except:
            return None

    return None

def update_existing_college_placements(limit=None):
    colleges = fetch_existing_colleges_for_placement_update(limit=limit)
    if not colleges:
        print("No existing colleges found for placement update.")
        return

    print(f"Updating placement section for {len(colleges)} existing colleges...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        try:
            for index, college in enumerate(colleges, start=1):
                college_id = college.get("source_college_id")
                college_url = (college.get("url") or "").strip()
                college_name = (
                    college.get("basic", {}).get("name")
                    if isinstance(college.get("basic"), dict)
                    else ""
                )

                if not college_id or not college_url:
                    print(f"[{index}/{len(colleges)}] Skipping invalid college record: {college}")
                    continue

                label = college_name or college_url
                print(f"[{index}/{len(colleges)}] Updating placement for: {label}")

                try:
                    if not safe_goto(page, college_url):
                        print(f"Failed to open college page: {college_url}")
                        continue

                    placement_data = scrape_placement_page(page)
                    update_mongo_section(college_id, "placement", placement_data)
                    print(
                        "Placement updated:",
                        college_id,
                        placement_data.get("package_highlights", {})
                    )
                except Exception as e:
                    print(f"Placement update failed for {college_url}: {e}")
        finally:
            browser.close()
# ---------------- MAIN ----------------
def main(target_url="", output_file=""):
    active_url = _resolve_runtime_url(target_url)
    active_output_file = _resolve_output_file(output_file)

    data = {
        "source": "collegedunia",
        "source_college_id": extract_college_id(active_url),
        "url": active_url,
        "basic": {
            "name": "",
            "logo": "",
            "city": "",
            "state": "",
            "college_type": "",
            "established_year": "",
            "rating": None,
            "reviews": None,
            "about": {},
        }
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        try:
            print("Navigating...")
            page.goto(active_url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_selector("h1", timeout=30000)

            scrape_basic_header(page, data["basic"])
            scrape_about_and_toc(page, data["basic"]) 
            update_mongo_section(
                data["source_college_id"],
                "basic",
                data["basic"]
            )

            # ---------- ADMISSION ----------
            if open_admission_tab(page):
                print("Ã°Å¸â€œËœ Scraping Admission page...")
                expand_read_more(page)
                admission_data = {}
                scrape_about_and_toc(page, admission_data)

                important_dates = scrape_important_dates(page)
                if important_dates.get("important_events") or important_dates.get("expired_events"):
                    admission_data["important_dates"] = important_dates

               # TOC click-based scraping
                toc_clicked = scrape_toc_by_clicking(page)
                if toc_clicked:
                    admission_data.setdefault("toc_sections", []).extend(toc_clicked)

                data["admission"] = admission_data
                update_mongo_section(
                    data["source_college_id"],
                    "admission",
                    admission_data
                )


            # ---------- REVIEWS ----------
            if open_reviews_tab(page):
                print("Ã¢Â­Â Scraping Reviews page...")
                reviews_data = scrape_reviews_page(page)
                data["reviews_page"] = reviews_data 
            update_mongo_section(
                data["source_college_id"],
                "reviews_page",
                reviews_data
            )

            ranking_data = scrape_ranking_page(page)
            data["ranking"] = ranking_data 
            update_mongo_section(
                data["source_college_id"],
                "ranking",
                ranking_data
            )

            # ---------- PLACEMENT ----------
            placement_data = scrape_placement_page(page)
            data["placement"] = placement_data
            update_mongo_section(
                data["source_college_id"],
                "placement",
                 placement_data
                )

            # ---------- FACULTY ----------
            faculty_data = scrape_faculty_page(page)
            data["faculty"] = faculty_data
            update_mongo_section(
                data["source_college_id"],
                "faculty",
                faculty_data
            )
                # ---------- CUTOFF ----------
            cutoff_data = scrape_cutoff_page(page)
            data["cutoff"] = cutoff_data
            update_mongo_section(
                data["source_college_id"],
                "cutoff",
                cutoff_data
            )
            scholarship_data = scrape_scholarship_page(page)
            data["scholarship"] = scholarship_data
            
            update_mongo_section(
                data["source_college_id"],
                "scholarship",
                scholarship_data
            )
            # ---------- GALLERY ----------
            print("Ã°Å¸â€“Â¼Ã¯Â¸Â Scraping Gallery images...")
            gallery_images = scrape_gallery_images(page, active_url)
            data["gallery"] = gallery_images 
            update_mongo_section(
                data["source_college_id"],
                "gallery",
                gallery_images
            )

                # ---------- QnA ----------
            
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(1500)
         


            data["qna"] = scrape_all_qna(
                page,
                extract_college_id(active_url)
            )
            update_mongo_section(
                data["source_college_id"],
                "qna",
                data["qna"]
            )


        except Exception as e:
            print("Error:", e)
            page.screenshot(path="error_debug.png")

        finally:
            browser.close()

    with open(active_output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    #save_to_mongo(data)

    print(f"Data saved to {active_output_file}")
    print(f"Data upserted to MongoDB collection: {MONGO_COLLECTION}")

if __name__ == "__main__":
    args = parse_args()
    if args.update_existing_placements:
        update_existing_college_placements(limit=args.limit)
    else:
        main(target_url=args.url, output_file=args.output_file)
