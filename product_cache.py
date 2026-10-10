#!/usr/bin/env python3
"""Farah Gold — Shopify product cache.

Maintains a local SQLite cache of SKU -> Shopify product/variant/inventory IDs
for Lab Jewelry and Lab-Grown Diamond products. Populated by:
  1. Initial full fetch (one-time, slow)
  2. Webhook updates (ongoing, instant)
  3. Periodic reconciliation (weekly, to catch drift)

The daily syncs use this cache instead of paginating through all products,
eliminating the MCP wrapper throttling problem.

Usage:
  product_cache.py --rebuild    Full rebuild from Shopify (slow, one-time)
  product_cache.py --lookup SKU  Look up a single SKU
  product_cache.py --stats       Show cache stats
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_DB = os.path.join(HERE, "product_cache.db")
SHOPIFY_CLI = os.environ.get("SHOPIFY_CLI", "shopify")


def get_db():
    db = sqlite3.connect(CACHE_DB)
    db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            sku TEXT PRIMARY KEY,
            product_id TEXT NOT NULL,
            variant_id TEXT NOT NULL,
            inventory_item_id TEXT NOT NULL,
            title TEXT,
            product_type TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS cache_meta (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    return db


def mcp_call(tool, arguments, tries=5):
    last_err = None
    for t in range(tries):
        proc = subprocess.run(
            [SHOPIFY_CLI, "call-tool", "--name", tool,
             "--arguments-json", json.dumps(arguments)],
            capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            last_err = proc.stderr[:300]
            time.sleep(10 * (t + 1))
            continue
        try:
            envelope = json.loads(proc.stdout)
        except Exception:
            last_err = "JSON parse failed"
            time.sleep(10 * (t + 1))
            continue
        if not envelope.get("ok"):
            last_err = proc.stdout[:300]
            time.sleep(10 * (t + 1))
            continue
        result = envelope.get("result", {})
        # Try structured content first
        data = result.get("structuredContent", {}).get("data")
        if data is not None:
            return data
        # Fall back to parsing text content
        for block in result.get("content", []):
            text = block.get("text", "")
            # Look for DATA-RETURNED marker with JSON
            import re
            m = re.search(r'\{"data":\{.*\}\}', text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(0)).get("data", {})
                except Exception:
                    continue
        last_err = "No data in response"
        time.sleep(10 * (t + 1))
    raise RuntimeError(f"Shopify call failed: {last_err}")


def wait_for_throttle_clear(max_wait=600):
    """Wait for MCP wrapper throttling to clear."""
    start = time.time()
    attempt = 0
    while time.time() - start < max_wait:
        attempt += 1
        try:
            d = mcp_call("graphql_query", {"query": "{ shop { name } }"}, tries=1)
            if d and "shop" in d:
                return True
        except RuntimeError as e:
            if "could not be parsed as one" in str(e):
                wait = min(60, max_wait - (time.time() - start))
                if wait <= 0:
                    break
                print(f"  throttled, waiting {wait:.0f}s... (attempt {attempt})")
                time.sleep(wait)
                continue
            raise
    return False


def rebuild_cache():
    """Full rebuild: paginate through all relevant products and cache them."""
    db = get_db()

    # Clear existing
    db.execute("DELETE FROM products")
    db.commit()

    for product_type in ["Lab Jewelry", "Lab-Grown Diamond"]:
        print(f"Fetching {product_type} products...")
        cursor = None
        count = 0
        consecutive_throttles = 0

        while True:
            after = f' after:"{cursor}"' if cursor else ""
            q = (f'{{ products(first: 100{after}, '
                 f'query:"product_type:{product_type}") {{'
                 ' pageInfo { hasNextPage endCursor }'
                 ' nodes { id title productType variants(first: 1) { nodes {'
                 ' id sku inventoryItem { id } } } } } }')

            try:
                d = mcp_call("graphql_query", {"query": q})
                consecutive_throttles = 0
            except RuntimeError as e:
                if "could not be parsed as one" in str(e):
                    consecutive_throttles += 1
                    if consecutive_throttles > 5:
                        print(f"  Too many throttles, aborting {product_type}")
                        break
                    print(f"  throttled, waiting...")
                    if not wait_for_throttle_clear():
                        break
                    continue
                raise

            nodes = d.get("products", {}).get("nodes", [])
            for n in nodes:
                variants = n.get("variants", {}).get("nodes", [])
                if not variants:
                    continue
                v = variants[0]
                sku = v.get("sku")
                if not sku:
                    continue
                db.execute(
                    "INSERT OR REPLACE INTO products "
                    "(sku, product_id, variant_id, inventory_item_id, title, product_type) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (sku, n["id"], v["id"],
                     v.get("inventoryItem", {}).get("id", ""),
                     n.get("title", ""), n.get("productType", product_type))
                )
                count += 1

            db.commit()

            page_info = d.get("products", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(5)  # pacing

        print(f"  Cached {count} {product_type} products")

    db.execute(
        "INSERT OR REPLACE INTO cache_meta (key, value) VALUES (?, ?)",
        ("last_rebuild", time.strftime("%Y-%m-%dT%H:%M:%S"))
    )
    db.commit()

    total = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    print(f"\nTotal cached: {total} products")
    db.close()


def lookup(sku):
    db = get_db()
    row = db.execute(
        "SELECT sku, product_id, variant_id, inventory_item_id, title, product_type "
        "FROM products WHERE sku = ?", (sku,)
    ).fetchone()
    db.close()
    if row:
        return {
            "sku": row[0], "product_id": row[1], "variant_id": row[2],
            "inventory_item_id": row[3], "title": row[4], "product_type": row[5],
        }
    return None


def update_from_webhook(sku, product_id, variant_id, inventory_item_id, title, product_type, deleted=False):
    """Called by the webhook receiver when Shopify notifies of a change."""
    db = get_db()
    if deleted:
        db.execute("DELETE FROM products WHERE sku = ?", (sku,))
        print(f"Removed {sku} from cache (deleted in Shopify)")
    else:
        db.execute(
            "INSERT OR REPLACE INTO products "
            "(sku, product_id, variant_id, inventory_item_id, title, product_type) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (sku, product_id, variant_id, inventory_item_id, title, product_type)
        )
        print(f"Updated cache for {sku}")
    db.commit()
    db.close()


def stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    by_type = db.execute(
        "SELECT product_type, COUNT(*) FROM products GROUP BY product_type"
    ).fetchall()
    last_rebuild = db.execute(
        "SELECT value FROM cache_meta WHERE key = 'last_rebuild'"
    ).fetchone()
    db.close()

    print(f"Total cached products: {total}")
    for pt, count in by_type:
        print(f"  {pt}: {count}")
    if last_rebuild:
        print(f"Last rebuild: {last_rebuild[0]}")
    print(f"Cache DB: {CACHE_DB}")


def main():
    ap = argparse.ArgumentParser(description="Shopify product cache")
    ap.add_argument("--rebuild", action="store_true", help="Full rebuild from Shopify")
    ap.add_argument("--lookup", metavar="SKU", help="Look up a single SKU")
    ap.add_argument("--stats", action="store_true", help="Show cache stats")
    args = ap.parse_args()

    if args.rebuild:
        rebuild_cache()
    elif args.lookup:
        result = lookup(args.lookup)
        if result:
            print(json.dumps(result, indent=2))
        else:
            print(f"SKU not in cache: {args.lookup}")
    elif args.stats:
        stats()
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
