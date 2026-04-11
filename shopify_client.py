"""
Shopify API client for fetching orders.
Also imports orders from CSV export as fallback.
"""
import os, re, csv, datetime
from datetime import date
import requests
from dotenv import load_dotenv
import models, catalog

load_dotenv()

STORE = os.getenv("SHOPIFY_STORE", "antiquajoyeria.myshopify.com")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-04")
CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET", "")

# Token cache
_access_token = None


def get_access_token():
    """Get access token using client credentials grant."""
    global _access_token
    if _access_token:
        return _access_token

    # Try env var first
    token = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
    if token:
        _access_token = token
        return token

    # OAuth client credentials
    url = f"https://{STORE}/admin/oauth/access_token"
    resp = requests.post(url, json={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    })
    if resp.status_code == 200:
        _access_token = resp.json().get("access_token")
        return _access_token

    print(f"Failed to get token: {resp.status_code} {resp.text}")
    return None


CHAIN_NAMES = {"forza", "rolo", "3x1"}  # Basic factory chains only

# Order numbers to exclude from sync (e.g. orders from prior year)
EXCLUDED_ORDER_NUMBERS = {"#1822"}

# Standalone service items (not real jewelry orders)
_SERVICE_PATTERNS = [
    r'^grabado$',
    r'^talla\b',
    r'^cambio\s+de?\s*talla$',
    r'^ajuste\s+de?\s*talla$',
]
_SERVICE_RE = re.compile("|".join(_SERVICE_PATTERNS), re.IGNORECASE)


def is_service_item(product_name: str) -> bool:
    """Returns True if the item is a standalone service (grabado, talla adjustment), not a jewelry order."""
    return bool(_SERVICE_RE.match(product_name.strip()))


def classify_product_type(product_name: str) -> str:
    """Classify product as 'joya', 'joyero' (Relique Box), 'cadena' (basic chains), or 'servicio'."""
    lower = product_name.lower().strip()
    if is_service_item(product_name):
        return "servicio"
    if "relique" in lower or "box" in lower:
        return "joyero"
    base = re.split(r'\s*-\s*', lower)[0].strip()
    if base in CHAIN_NAMES:
        return "cadena"
    return "joya"


# ── Partial payment detection ──
_PARTIAL_PATTERNS = [
    r"first\s*payment", r"second\s*payment",
    r"primer\s*pago", r"segundo\s*pago",
    r"pago\s*1\b", r"pago\s*2\b",
    r"1er\s*pago", r"2do\s*pago", r"2º\s*pago",
]
_PARTIAL_RE = re.compile("|".join(_PARTIAL_PATTERNS), re.IGNORECASE)


def is_partial_payment(item_name: str) -> bool:
    """Returns True if the item name indicates a split/partial payment."""
    return bool(_PARTIAL_RE.search(item_name))


_CLEAN_PATTERNS = [
    r"\s*-?\s*first\s*payment",
    r"\s*-?\s*second\s*payment",
    r"\s*-?\s*primer\s*pago",
    r"\s*-?\s*segundo\s*pago",
    r"\s*-?\s*pago\s*1\b",
    r"\s*-?\s*pago\s*2\b",
    r"\s*-?\s*1er\s*pago",
    r"\s*-?\s*2do\s*pago",
    r"\s*-?\s*2º\s*pago",
]
_CLEAN_RE = re.compile("|".join(_CLEAN_PATTERNS), re.IGNORECASE)


def clean_payment_name(item_name: str) -> str:
    """Strip payment indicators and normalize the product name."""
    cleaned = _CLEAN_RE.sub("", item_name)
    # Normalize dashes and spaces
    cleaned = re.sub(r'\s*-\s*$', '', cleaned)  # trailing dash
    cleaned = re.sub(r'^\s*-\s*', '', cleaned)  # leading dash
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def parse_line_item(name: str):
    """
    Parse Shopify line item name into product, size, variant.
    Examples:
      "Aluna - 17"           -> ("Aluna", "17", None)
      "Gorgonia - 12 / Rubi" -> ("Gorgonia", "12", "Rubi")
      "Curie 7,5"            -> ("Curie", "7,5", None)
      "Malala"               -> ("Malala", None, None)
    """
    name = name.strip()

    # Pattern: "Name - Size / Variant"
    m = re.match(r'^(.+?)\s*-\s*(\d[\d,\.]*)\s*/\s*(.+)$', name)
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()

    # Pattern: "Name - Size"
    m = re.match(r'^(.+?)\s*-\s*(\d[\d,\.]*)$', name)
    if m:
        return m.group(1).strip(), m.group(2).strip(), None

    # Pattern: "Name Size" (no dash, like "Curie 7,5")
    m = re.match(r'^(.+?)\s+(\d[\d,\.]+)$', name)
    if m:
        return m.group(1).strip(), m.group(2).strip(), None

    return name, None, None


def fetch_orders_api(since_id=None, limit=250, year=2026):
    """Fetch orders from Shopify REST API. Paginates to get ALL orders for the given year."""
    token = get_access_token()
    if not token:
        return []

    all_orders = []
    url = f"https://{STORE}/admin/api/{API_VERSION}/orders.json"
    params = {
        "limit": limit,
        "status": "any",
        "created_at_min": f"{year}-01-01T00:00:00Z",
        "fields": "id,name,email,created_at,financial_status,fulfillment_status,"
                  "total_price,subtotal_price,total_tax,line_items,billing_address,"
                  "shipping_address,shipping_lines,note,tags,payment_gateway_names",
    }
    if since_id:
        params["since_id"] = since_id

    headers = {"X-Shopify-Access-Token": token}

    # Paginate through all results
    while url:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"Shopify API error: {resp.status_code} {resp.text[:200]}")
            break

        orders = resp.json().get("orders", [])
        all_orders.extend(orders)

        # Check for next page via Link header
        link_header = resp.headers.get("Link", "")
        if 'rel="next"' in link_header:
            # Extract next URL
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]
                    params = {}  # URL already contains params
                    break
        else:
            break

    print(f"Fetched {len(all_orders)} orders from Shopify API (year {year})")
    return all_orders


def sync_from_api(full=False):
    """Pull orders from Shopify API and create/update in DB.
    full=True: fetch ALL 2026 orders (ignore since_id). Used for initial load.
    full=False: fetch only new orders since last sync.
    """
    last_id = models.get_setting("last_shopify_id", "0")
    if full:
        orders = fetch_orders_api(since_id=None)
    else:
        orders = fetch_orders_api(since_id=last_id if last_id != "0" else None)

    gold_price = float(models.get_setting("gold_price", os.getenv("GOLD_PRICE_PER_GRAM", "160.0")))
    count = 0

    for order in orders:
        order_number = order.get("name", "")

        # Skip excluded orders (e.g. orders from prior year)
        if order_number in EXCLUDED_ORDER_NUMBERS:
            continue

        # Calculate shipping price paid by customer (shared across line items)
        shipping_lines = order.get("shipping_lines", [])
        shipping_method = shipping_lines[0].get("title", "") if shipping_lines else ""
        shipping_total = sum(float(s.get("price", 0)) for s in shipping_lines)
        line_items = order.get("line_items", [])
        num_items = len(line_items)

        for idx, item in enumerate(line_items):
            raw_item_name = item.get("name", "")
            product_name, ring_size, variant = parse_line_item(raw_item_name)
            pvp = float(item.get("price", 0)) * int(item.get("quantity", 1))

            # Get cost estimates from catalog
            estimates = catalog.estimate_costs(product_name, pvp, gold_price) or {}

            billing = order.get("billing_address") or {}
            shipping_addr = order.get("shipping_address") or {}
            addr = shipping_addr or billing

            # Detect pickup vs shipping
            es_recogida = "store" in shipping_method.lower() or "recogida" in shipping_method.lower() or "pickup" in shipping_method.lower()

            # Classify product type
            product_type = classify_product_type(product_name)

            # Assign shipping to first non-joyero item (or split if needed)
            if num_items == 1:
                item_shipping = shipping_total
            elif idx == 0 and product_type != "joyero":
                item_shipping = shipping_total
            else:
                item_shipping = 0.0

            # Detect partial payments
            partial = is_partial_payment(raw_item_name)
            customer_name = billing.get("name", "")
            payment_group = ""
            if partial:
                cleaned = clean_payment_name(raw_item_name)
                payment_group = f"{cleaned}|{customer_name}".lower().strip()

            # Payment gateway detection
            gateways = order.get("payment_gateway_names", [])
            gateway = gateways[0].lower() if gateways else ""
            # Auto-detect estado_pago: Shopify Payments, PayPal, Klarna = pagado; transferencia = pendiente
            auto_pago = "pagado"
            if "manual" in gateway or "bank" in gateway or "transfer" in gateway or not gateway:
                auto_pago = "pendiente"

            # Auto-cancel: Malena #1845 (financial_status=pending, confirmed cancelled)
            financial_status = order.get("financial_status", "")
            auto_status = "nuevo"
            if order_number == "#1845":
                auto_status = "cancelado"

            data = {
                "shopify_order_id": str(order["id"]),
                "shopify_order_number": order_number,
                "customer_name": customer_name,
                "customer_email": order.get("email", ""),
                "customer_phone": billing.get("phone", ""),
                "customer_address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('zip', '')}",
                "product_name": product_name,
                "product_type": product_type,
                "ring_size": ring_size,
                "variant": variant,
                "fecha_pedido": order.get("created_at", "")[:10],
                "pvp": pvp,
                "shipping_price": item_shipping,
                "is_partial_payment": "1" if partial else "0",
                "payment_group": payment_group if partial else "",
                "payment_gateway": gateway,
                "estado_pago": auto_pago,
                "metodo_envio": shipping_method,
                "es_recogida": "1" if es_recogida else "0",
                "status": auto_status,
                **estimates,
            }
            models.upsert_order(data)
            count += 1

        # Track last ID
        if str(order["id"]) > last_id:
            last_id = str(order["id"])

    models.set_setting("last_shopify_id", last_id)

    # Auto-match leads for newly synced orders
    _auto_match_leads_for_recent_orders()

    return count


def _auto_match_leads_for_recent_orders():
    """Check recent unmatched orders and try to link them to existing leads."""
    conn = models.get_db()
    # Get orders without a lead_id that haven't been checked
    unmatched = conn.execute(
        "SELECT id, customer_email, customer_phone, customer_name, shopify_order_id "
        "FROM orders WHERE lead_id IS NULL AND status='nuevo' "
        "ORDER BY created_at DESC LIMIT 50"
    ).fetchall()
    conn.close()

    for order in unmatched:
        lead_id = models.match_lead_to_order(
            order["customer_email"] or "",
            order["customer_phone"] or "",
            order["customer_name"] or ""
        )
        if lead_id:
            models.convert_lead(lead_id, order["shopify_order_id"] or "")
            models.update_order(order["id"], {"lead_id": lead_id})
            models.log_activity(order["id"], "Lead convertido automáticamente", f"Lead #{lead_id}")


def sync_from_csv(csv_path: str = None):
    """Import orders from Shopify CSV export (fallback if API not configured)."""
    if csv_path is None:
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "Negocio", "orders_export.csv"
        )

    csv_path = os.path.abspath(csv_path)
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        return 0

    gold_price = float(models.get_setting("gold_price", os.getenv("GOLD_PRICE_PER_GRAM", "160.0")))
    count = 0

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_item_name = row.get("Lineitem name", "")
            if not raw_item_name:
                continue

            product_name, ring_size, variant = parse_line_item(raw_item_name)
            pvp = float(row.get("Lineitem price", 0) or 0)

            estimates = catalog.estimate_costs(product_name, pvp, gold_price) or {}

            created = row.get("Created at", "")
            fecha = created[:10] if created else ""

            # Classify product type
            product_type = classify_product_type(product_name)

            # Detect partial payments
            partial = is_partial_payment(raw_item_name)
            customer_name = row.get("Billing Name", "")
            payment_group = ""
            if partial:
                cleaned = clean_payment_name(raw_item_name)
                payment_group = f"{cleaned}|{customer_name}".lower().strip()

            data = {
                "shopify_order_id": row.get("Id", row.get("Name", "")),
                "shopify_order_number": row.get("Name", ""),
                "customer_name": customer_name,
                "customer_email": row.get("Email", ""),
                "customer_phone": row.get("Billing Phone", row.get("Phone", "")),
                "customer_address": f"{row.get('Shipping Address1', '')}, {row.get('Shipping City', '')} {row.get('Shipping Zip', '')}",
                "product_name": product_name,
                "product_type": product_type,
                "ring_size": ring_size,
                "variant": variant,
                "fecha_pedido": fecha,
                "pvp": pvp,
                "is_partial_payment": "1" if partial else "0",
                "payment_group": payment_group if partial else "",
                "status": "nuevo",
                **estimates,
            }
            models.upsert_order(data)
            count += 1

    print(f"Imported {count} orders from CSV")
    return count


def process_webhook_order(order_data: dict) -> list[int]:
    """Process a single order from Shopify webhook. Returns list of created order IDs."""
    gold_price = float(models.get_setting("gold_price", os.getenv("GOLD_PRICE_PER_GRAM", "160.0")))
    order_ids = []

    order_number = order_data.get("name", "")
    if order_number in EXCLUDED_ORDER_NUMBERS:
        return order_ids

    # Calculate shipping
    shipping_lines = order_data.get("shipping_lines", [])
    shipping_method = shipping_lines[0].get("title", "") if shipping_lines else ""
    shipping_total = sum(float(s.get("price", 0)) for s in shipping_lines)
    line_items = order_data.get("line_items", [])
    num_items = len(line_items)

    for idx, item in enumerate(line_items):
        raw_item_name = item.get("name", "")
        product_name, ring_size, variant = parse_line_item(raw_item_name)
        pvp = float(item.get("price", 0)) * int(item.get("quantity", 1))

        estimates = catalog.estimate_costs(product_name, pvp, gold_price) or {}

        billing = order_data.get("billing_address") or {}
        shipping_addr = order_data.get("shipping_address") or {}
        addr = shipping_addr or billing

        es_recogida = "store" in shipping_method.lower() or "recogida" in shipping_method.lower() or "pickup" in shipping_method.lower()

        product_type = classify_product_type(product_name)

        # Assign shipping to first non-joyero item
        if num_items == 1:
            item_shipping = shipping_total
        elif idx == 0 and product_type != "joyero":
            item_shipping = shipping_total
        else:
            item_shipping = 0.0

        partial = is_partial_payment(raw_item_name)
        customer_name = billing.get("name", "")
        payment_group = ""
        if partial:
            cleaned = clean_payment_name(raw_item_name)
            payment_group = f"{cleaned}|{customer_name}".lower().strip()

        # Payment gateway detection
        gateways = order_data.get("payment_gateway_names", [])
        gateway = gateways[0].lower() if gateways else ""
        auto_pago = "pagado"
        if "manual" in gateway or "bank" in gateway or "transfer" in gateway or not gateway:
            auto_pago = "pendiente"

        data = {
            "shopify_order_id": str(order_data["id"]),
            "shopify_order_number": order_number,
            "customer_name": customer_name,
            "customer_email": order_data.get("email", ""),
            "customer_phone": billing.get("phone", ""),
            "customer_address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('zip', '')}",
            "product_name": product_name,
            "product_type": product_type,
            "ring_size": ring_size,
            "variant": variant,
            "fecha_pedido": order_data.get("created_at", "")[:10],
            "pvp": pvp,
            "shipping_price": item_shipping,
            "is_partial_payment": "1" if partial else "0",
            "payment_group": payment_group if partial else "",
            "payment_gateway": gateway,
            "estado_pago": auto_pago,
            "metodo_envio": shipping_method,
            "es_recogida": "1" if es_recogida else "0",
            "status": "nuevo",
            **estimates,
        }
        oid = models.upsert_order(data)
        order_ids.append(oid)

    return order_ids


if __name__ == "__main__":
    models.init_db()
    catalog.load_catalog()
    # Try API first, fallback to CSV
    n = sync_from_api()
    if n == 0:
        print("API returned 0 orders, trying CSV import...")
        n = sync_from_csv()
    print(f"Total synced: {n}")
