#!/usr/bin/env python3
"""Farah Gold — Shopify webhook receiver.

Receives product create/update/delete webhooks from Shopify and updates
the local product cache (product_cache.py) so daily syncs don't need to
paginate through all products.

Endpoints:
  POST /webhooks/products/create  — new product created
  POST /webhooks/products/update  — product updated
  POST /webhooks/products/delete  — product deleted
  GET  /health                     — health check

Webhook verification: validates HMAC-SHA256 signature using SHOPIFY_WEBHOOK_SECRET.
"""

import base64
import hashlib
import hmac
import json
import os
import sys

from flask import Flask, request, jsonify

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from product_cache import update_from_webhook

app = Flask(__name__)
WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")


def verify_webhook(data, hmac_header):
    """Verify the webhook came from Shopify."""
    if not WEBHOOK_SECRET:
        print("WARNING: no webhook secret set, skipping verification")
        return True
    digest = hmac.new(
        WEBHOOK_SECRET.encode("utf-8"), data, hashlib.sha256
    ).digest()
    computed = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(computed, hmac_header)


def process_product(product_data, deleted=False):
    """Extract SKU and IDs from webhook payload, update cache."""
    variants = product_data.get("variants", [])
    if not variants:
        return

    for v in variants:
        sku = v.get("sku")
        if not sku:
            continue
        update_from_webhook(
            sku=sku,
            product_id=f"gid://shopify/Product/{product_data['id']}",
            variant_id=f"gid://shopify/ProductVariant/{v['id']}",
            inventory_item_id=f"gid://shopify/InventoryItem/{v.get('inventory_item_id', '')}",
            title=product_data.get("title", ""),
            product_type=product_data.get("product_type", ""),
            deleted=deleted,
        )


@app.route("/webhooks/products/create", methods=["POST"])
def product_create():
    if not verify_webhook(request.data, request.headers.get("X-Shopify-Hmac-Sha256", "")):
        return jsonify({"error": "invalid signature"}), 401
    process_product(request.json)
    return jsonify({"ok": True})


@app.route("/webhooks/products/update", methods=["POST"])
def product_update():
    if not verify_webhook(request.data, request.headers.get("X-Shopify-Hmac-Sha256", "")):
        return jsonify({"error": "invalid signature"}), 401
    process_product(request.json)
    return jsonify({"ok": True})


@app.route("/webhooks/products/delete", methods=["POST"])
def product_delete():
    if not verify_webhook(request.data, request.headers.get("X-Shopify-Hmac-Sha256", "")):
        return jsonify({"error": "invalid signature"}), 401
    process_product(request.json, deleted=True)
    return jsonify({"ok": True})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "farah-shopify-webhooks"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
