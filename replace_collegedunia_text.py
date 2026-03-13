import argparse
import re
from pymongo import MongoClient, ReplaceOne


DEFAULT_MONGO_URI = "mongodb+srv://alishakhan8488_db_user:DaVHn9goL8STNzNs@cluster0.nkmbpqt.mongodb.net/studentcap?retryWrites=true&w=majority"
DEFAULT_DB = "studentcap"
DEFAULT_COLLECTIONS = ["college_course", "new_college", "maincourse"]
DEFAULT_SOURCE_TEXT = "collegedunia"
DEFAULT_TARGET_TEXT = "studycups"
URL_FIELD_HINTS = {"url", "src", "href", "logo", "image", "icon", "thumbnail"}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replace text recursively inside MongoDB documents."
    )
    parser.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI, help="MongoDB URI")
    parser.add_argument("--db", default=DEFAULT_DB, help="Database name")
    parser.add_argument(
        "--collections",
        nargs="+",
        default=DEFAULT_COLLECTIONS,
        help="Collection names to process",
    )
    parser.add_argument(
        "--source-text",
        default=DEFAULT_SOURCE_TEXT,
        help="Text to replace (case-insensitive)",
    )
    parser.add_argument(
        "--target-text",
        default=DEFAULT_TARGET_TEXT,
        help="Replacement text",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Bulk write batch size",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show what would change, do not update MongoDB",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max docs per collection (0 means no limit)",
    )
    parser.add_argument(
        "--skip-url-repair",
        action="store_true",
        help="Skip repairing URL domains from target-text back to source-text.",
    )
    return parser.parse_args()


def _is_url_like_string(value, parent_key=""):
    if not isinstance(value, str):
        return False

    key = (parent_key or "").lower()
    if any(h in key for h in URL_FIELD_HINTS):
        return True

    v = value.strip().lower()
    if v.startswith("http://") or v.startswith("https://") or v.startswith("www."):
        return True
    if "://" in v:
        return True
    if "collegedunia.com" in v or "studycups.com" in v:
        return True
    return False


def transform_value(value, text_pattern, source_text, target_text, parent_key="", repair_urls=True):
    if isinstance(value, str):
        updated = value
        repaired_urls = 0
        text_replacements = 0

        is_url = _is_url_like_string(value, parent_key=parent_key)
        if repair_urls and is_url:
            updated, repaired_urls = re.subn(
                re.escape(target_text), source_text, updated, flags=re.IGNORECASE
            )

        if not is_url:
            updated, text_replacements = text_pattern.subn(target_text, updated)

        return updated, repaired_urls, text_replacements

    if isinstance(value, list):
        new_list = []
        repaired_urls = 0
        text_replacements = 0
        for item in value:
            updated_item, repaired, replaced = transform_value(
                item,
                text_pattern=text_pattern,
                source_text=source_text,
                target_text=target_text,
                parent_key=parent_key,
                repair_urls=repair_urls,
            )
            new_list.append(updated_item)
            repaired_urls += repaired
            text_replacements += replaced
        return new_list, repaired_urls, text_replacements

    if isinstance(value, dict):
        new_dict = {}
        repaired_urls = 0
        text_replacements = 0
        for key, item in value.items():
            if key == "_id":
                new_dict[key] = item
                continue
            updated_item, repaired, replaced = transform_value(
                item,
                text_pattern=text_pattern,
                source_text=source_text,
                target_text=target_text,
                parent_key=key,
                repair_urls=repair_urls,
            )
            new_dict[key] = updated_item
            repaired_urls += repaired
            text_replacements += replaced
        return new_dict, repaired_urls, text_replacements

    return value, 0, 0


def process_collection(
    db,
    name,
    text_pattern,
    source_text,
    target_text,
    dry_run=False,
    batch_size=200,
    limit=0,
    repair_urls=True,
):
    coll = db[name]
    cursor = coll.find({})
    if isinstance(limit, int) and limit > 0:
        cursor = cursor.limit(limit)

    scanned = 0
    changed_docs = 0
    repaired_url_count = 0
    text_replacements = 0
    bulk_ops = []

    for doc in cursor:
        scanned += 1
        updated_doc, repaired, replaced = transform_value(
            doc,
            text_pattern=text_pattern,
            source_text=source_text,
            target_text=target_text,
            repair_urls=repair_urls,
        )
        if repaired <= 0 and replaced <= 0:
            continue

        changed_docs += 1
        repaired_url_count += repaired
        text_replacements += replaced

        if dry_run:
            continue

        bulk_ops.append(ReplaceOne({"_id": doc["_id"]}, updated_doc))
        if len(bulk_ops) >= batch_size:
            coll.bulk_write(bulk_ops, ordered=False)
            bulk_ops = []

    if bulk_ops and not dry_run:
        coll.bulk_write(bulk_ops, ordered=False)

    return {
        "collection": name,
        "scanned": scanned,
        "changed_docs": changed_docs,
        "repaired_urls": repaired_url_count,
        "text_replacements": text_replacements,
        "replacements": repaired_url_count + text_replacements,
    }


def main():
    args = parse_args()
    text_pattern = re.compile(re.escape(args.source_text), flags=re.IGNORECASE)
    repair_urls = not args.skip_url_repair

    client = MongoClient(args.mongo_uri)
    try:
        db = client[args.db]

        print(
            f"DB={args.db} | collections={args.collections} | "
            f"replace-text '{args.source_text}' -> '{args.target_text}' | "
            f"repair_urls={repair_urls} | dry_run={args.dry_run}"
        )

        total_scanned = 0
        total_changed = 0
        total_replacements = 0
        total_repaired_urls = 0
        total_text_replacements = 0

        for collection_name in args.collections:
            stats = process_collection(
                db=db,
                name=collection_name,
                text_pattern=text_pattern,
                source_text=args.source_text,
                target_text=args.target_text,
                dry_run=args.dry_run,
                batch_size=args.batch_size,
                limit=args.limit,
                repair_urls=repair_urls,
            )
            total_scanned += stats["scanned"]
            total_changed += stats["changed_docs"]
            total_replacements += stats["replacements"]
            total_repaired_urls += stats["repaired_urls"]
            total_text_replacements += stats["text_replacements"]

            print(
                f"[{stats['collection']}] scanned={stats['scanned']} "
                f"changed_docs={stats['changed_docs']} "
                f"url_repairs={stats['repaired_urls']} "
                f"text_replacements={stats['text_replacements']} "
                f"total_changes={stats['replacements']}"
            )

        print(
            f"Done. total_scanned={total_scanned} "
            f"total_changed_docs={total_changed} "
            f"total_url_repairs={total_repaired_urls} "
            f"total_text_replacements={total_text_replacements} "
            f"total_replacements={total_replacements}"
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
