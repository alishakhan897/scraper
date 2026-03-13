import argparse
import ast
import os
import re

from pymongo import MongoClient, ReplaceOne


SCRIPT_CANDIDATES = [
    "basic_college_courses.py",
    "scraper_basic_college_course.py",
]
DEFAULT_BATCH_SIZE = 200
OLD_PROFILE_URL_PATTERN = re.compile(
    r"^https://image-static\.collegedunia\.com/public/image/.+collegedunia(?:%20|\s)+team\.jpeg\?h=35&w=35&mode=stretch$",
    flags=re.IGNORECASE,
)
NEW_PROFILE_URL = "https://png.pngtree.com/element_pic/00/16/09/2057e0eecf792fb.jpg"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replace Collegedunia team profile image URLs in MongoDB documents."
    )
    parser.add_argument(
        "--config-script",
        default="",
        help="Script path to read MONGO_URI, MONGO_DB and MONGO_COLLECTION from.",
    )
    parser.add_argument("--mongo-uri", default="", help="Override MongoDB URI")
    parser.add_argument("--db", default="", help="Override database name")
    parser.add_argument("--collection", default="", help="Override collection name")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Bulk write batch size",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max documents to scan (0 means no limit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show how many documents would change without updating MongoDB.",
    )
    return parser.parse_args()


def _pick_config_script(explicit_path=""):
    candidates = [explicit_path] if explicit_path else SCRIPT_CANDIDATES
    for path in candidates:
        if path and os.path.exists(path):
            return path
    raise FileNotFoundError(
        "Could not find config script. Tried: "
        + ", ".join(path for path in candidates if path)
    )


def _load_mongo_settings(script_path):
    with open(script_path, "r", encoding="utf-8-sig") as file_obj:
        tree = ast.parse(file_obj.read(), filename=script_path)

    required_names = {"MONGO_URI", "MONGO_DB", "MONGO_COLLECTION"}
    values = {}

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in required_names:
                values[target.id] = node.value.value

    missing = sorted(required_names - values.keys())
    if missing:
        raise ValueError(
            f"Missing Mongo settings in {script_path}: {', '.join(missing)}"
        )

    return values


def _replace_profile_url(value):
    if isinstance(value, str):
        if OLD_PROFILE_URL_PATTERN.fullmatch(value.strip()):
            return NEW_PROFILE_URL, 1
        return value, 0

    if isinstance(value, list):
        updated_items = []
        replacements = 0
        for item in value:
            updated_item, item_replacements = _replace_profile_url(item)
            updated_items.append(updated_item)
            replacements += item_replacements
        return updated_items, replacements

    if isinstance(value, dict):
        updated_dict = {}
        replacements = 0
        for key, item in value.items():
            updated_item, item_replacements = _replace_profile_url(item)
            updated_dict[key] = updated_item
            replacements += item_replacements
        return updated_dict, replacements

    return value, 0


def process_collection(collection, dry_run=False, batch_size=DEFAULT_BATCH_SIZE, limit=0):
    cursor = collection.find({})
    if isinstance(limit, int) and limit > 0:
        cursor = cursor.limit(limit)

    scanned = 0
    changed_docs = 0
    replaced_urls = 0
    bulk_ops = []

    for doc in cursor:
        scanned += 1
        updated_doc, replacements = _replace_profile_url(doc)
        if replacements <= 0:
            continue

        changed_docs += 1
        replaced_urls += replacements

        if dry_run:
            continue

        bulk_ops.append(ReplaceOne({"_id": doc["_id"]}, updated_doc))
        if len(bulk_ops) >= batch_size:
            collection.bulk_write(bulk_ops, ordered=False)
            bulk_ops = []

    if bulk_ops and not dry_run:
        collection.bulk_write(bulk_ops, ordered=False)

    return scanned, changed_docs, replaced_urls


def main():
    args = parse_args()
    config_script = _pick_config_script(args.config_script)
    mongo_settings = _load_mongo_settings(config_script)

    mongo_uri = args.mongo_uri or mongo_settings["MONGO_URI"]
    db_name = args.db or mongo_settings["MONGO_DB"]
    collection_name = args.collection or mongo_settings["MONGO_COLLECTION"]

    print(
        f"Using config from {config_script} | db={db_name} | "
        f"collection={collection_name} | dry_run={args.dry_run}"
    )
    print(f"Replacing matched profile URL with: {NEW_PROFILE_URL}")

    client = MongoClient(mongo_uri)
    try:
        collection = client[db_name][collection_name]
        scanned, changed_docs, replaced_urls = process_collection(
            collection=collection,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            limit=args.limit,
        )
    finally:
        client.close()

    print(
        f"Done. scanned={scanned} changed_docs={changed_docs} "
        f"replaced_urls={replaced_urls}"
    )


if __name__ == "__main__":
    main()
