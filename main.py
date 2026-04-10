import os, secrets
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException, status, UploadFile, File
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from jinja2 import Environment, FileSystemLoader
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from models import (
    init_db, get_db, get_order, get_all_orders, update_order,
    log_activity, get_activity, get_dashboard_stats,
    get_supplier_summary, get_setting, set_setting, get_all_products,
    get_supplier_orders, mark_piedras_entregadas, mark_joya_terminada,
    insert_bank_entry, get_bank_entries, update_bank_entry,
    insert_cash_sale, get_cash_sales, delete_cash_sale,
    get_accounting_stats,
    insert_lead, get_lead, get_all_leads, update_lead, get_lead_stats,
    match_lead_to_order, convert_lead, advance_production_phase,
    PRODUCTION_PHASES,
)
from bank_reconciliation import parse_bank_csv, categorize_entry, match_with_orders, get_unmatched_summary
from catalog import load_catalog, estimate_costs
from shopify_client import sync_from_api, sync_from_csv
from excel_sync import export_order_to_excel, import_excel_to_db, export_all_to_excel
from notifications import notify_supplier, get_notification_preview, SUPPLIERS, notify_customer, CUSTOMER_TEMPLATES
from gold_price import get_gold_info, get_18k_gold_price, get_current_gold_price

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = int(os.getenv("PORT", 8000))
DEFAULT_GOLD_PRICE = float(os.getenv("GOLD_PRICE_PER_GRAM", 160.0))

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Antiqua - Gestión de Pedidos")

SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(32))
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")

app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


class AuthRedirect(Exception):
    pass


@app.exception_handler(AuthRedirect)
async def auth_redirect_handler(request: Request, exc: AuthRedirect):
    return RedirectResponse("/login", status_code=302)


def require_auth(request: Request):
    """Check if user is authenticated via session."""
    if not DASHBOARD_PASSWORD:
        return
    if request.session.get("authenticated"):
        return
    raise AuthRedirect()


@app.get("/login")
def login_page(request: Request, error: str = None):
    return templates.TemplateResponse("login.html", {"request": request, "error": error})


@app.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    password = form.get("password", "")
    if password == DASHBOARD_PASSWORD:
        request.session["authenticated"] = True
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Contraseña incorrecta"})

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
def on_startup():
    init_db()
    try:
        load_catalog()
    except Exception as e:
        print(f"Catalog not loaded (OK for cloud deploy): {e}")
    try:
        gold_info = get_gold_info()
        print(f"Gold price at startup: 24k={gold_info['price_24k_gram']} EUR/g (source: {gold_info['source']})")
    except Exception as e:
        print(f"Could not fetch gold price at startup: {e}")
    # Auto-sync orders from Shopify API on startup (Shopify = single source of truth)
    try:
        stats = get_dashboard_stats()
        if stats.get("total", 0) == 0:
            # DB is empty — full sync from Shopify API
            n = sync_from_api(full=True)
            if n > 0:
                print(f"Auto-synced {n} orders from Shopify API")
            else:
                # Fallback to CSV if API fails
                csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "orders_export.csv")
                if os.path.exists(csv_path):
                    n = sync_from_csv(csv_path)
                    print(f"Fallback: imported {n} orders from CSV")
    except Exception as e:
        print(f"Auto-import failed: {e}")

    # Fix product_type for orders missing it (ensures merge works correctly)
    try:
        from shopify_client import classify_product_type
        conn = get_db()
        orders = conn.execute("SELECT id, product_name FROM orders WHERE product_type IS NULL OR product_type=''").fetchall()
        for o in orders:
            pt = classify_product_type(o["product_name"] or "")
            conn.execute("UPDATE orders SET product_type=? WHERE id=?", (pt, o["id"]))
        if orders:
            conn.commit()
            print(f"Fixed product_type for {len(orders)} orders")
        conn.close()
    except Exception as e:
        print(f"Product type fix failed: {e}")

# ---------------------------------------------------------------------------
# HTML Pages
# ---------------------------------------------------------------------------
@app.get("/", dependencies=[Depends(require_auth)])
def dashboard(request: Request, status: str = None, month: str = None, search: str = None, page: int = 1):
    try:
        filters = {}
        if status:
            filters["status"] = status
        if month:
            filters["month"] = month
        if search:
            filters["search"] = search

        all_orders = get_all_orders(**filters)
        stats = get_dashboard_stats()
        gold_price_info = get_gold_info()

        # Pagination: 100 per page
        per_page = 100
        total_orders = len(all_orders)
        total_pages = max(1, (total_orders + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        start = (page - 1) * per_page
        orders = all_orders[start:start + per_page]

        return templates.TemplateResponse(name="dashboard.html", request=request, context={
            "orders": orders,
            "stats": stats,
            "gold_price": gold_price_info,
            "current_status": status or "",
            "current_month": month or "",
            "current_search": search or "",
            "page": page,
            "total_pages": total_pages,
            "total_orders": total_orders,
        })
    except Exception as e:
        return templates.TemplateResponse(name="dashboard.html", request=request, context={
            "orders": [],
            "stats": {},
            "gold_price": get_gold_info(),
            "current_status": "",
            "current_month": "",
            "current_search": "",
            "error": str(e),
        })


@app.get("/order/{order_id}", dependencies=[Depends(require_auth)])
def order_detail(request: Request, order_id: str):
    try:
        order = get_order(order_id)
        activity_log = get_activity(order_id)
        notification_preview = get_notification_preview(order)
        catalog_match = estimate_costs(order.get("product_name"), order.get("pvp")) if order else None

        return templates.TemplateResponse(name="order.html", request=request, context={
            "order": order,
            "activity_log": activity_log,
            "notification_preview": notification_preview,
            "catalog_match": catalog_match,
        })
    except Exception as e:
        print(f"Order detail error for {order_id}: {e}")
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=f"/?error=Pedido+{order_id}+no+encontrado", status_code=302)


@app.get("/products", dependencies=[Depends(require_auth)])
def products_page(request: Request):
    try:
        products = get_all_products()
        return templates.TemplateResponse(name="products.html", request=request, context={
            "products": products,
        })
    except Exception as e:
        return templates.TemplateResponse(name="products.html", request=request, context={
            "products": [],
            "error": str(e),
        })


@app.get("/calculadora", dependencies=[Depends(require_auth)])
def calculadora_page(request: Request):
    try:
        products = get_all_products()
        gold_price_info = get_gold_info()
        return templates.TemplateResponse(name="calculadora.html", request=request, context={
            "products": products,
            "gold_price": gold_price_info,
        })
    except Exception as e:
        return templates.TemplateResponse(name="calculadora.html", request=request, context={
            "products": [],
            "gold_price": get_gold_info(),
            "error": str(e),
        })


@app.get("/api/products", dependencies=[Depends(require_auth)])
def api_products():
    try:
        products = get_all_products()
        return JSONResponse(products)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/suppliers", dependencies=[Depends(require_auth)])
def suppliers_page(request: Request):
    try:
        lola_summary = get_supplier_summary("lola")
        barto_summary = get_supplier_summary("barto")
        return templates.TemplateResponse(name="suppliers.html", request=request, context={
            "lola_summary": lola_summary,
            "barto_summary": barto_summary,
        })
    except Exception as e:
        return templates.TemplateResponse(name="suppliers.html", request=request, context={
            "lola_summary": {},
            "barto_summary": {},
            "error": str(e),
        })

# ---------------------------------------------------------------------------
# Supplier Portal (public, no auth required)
# ---------------------------------------------------------------------------
@app.get("/proveedor/{supplier}")
def supplier_portal(request: Request, supplier: str):
    if supplier not in ("barto", "lola"):
        raise HTTPException(status_code=404, detail="Proveedor no encontrado")
    try:
        from datetime import date, timedelta
        from notifications import add_business_days, MESES

        data = get_supplier_orders(supplier)
        supplier_name = "Barto" if supplier == "barto" else "Lola"

        # Calculate deadline and urgency for each pending order
        today = date.today()
        for order in data["pending"]:
            fecha = order.get("fecha_pedido", "")
            if fecha and len(fecha) >= 10:
                try:
                    order_date = date.fromisoformat(fecha[:10])
                    deadline = add_business_days(order_date, 18)
                    days_left = (deadline - today).days
                    order["deadline"] = f"{deadline.day} de {MESES[deadline.month - 1]}"
                    order["days_left"] = days_left
                    if days_left < 0:
                        order["urgency"] = "overdue"
                    elif days_left < 5:
                        order["urgency"] = "urgent"
                    elif days_left <= 10:
                        order["urgency"] = "warning"
                    else:
                        order["urgency"] = "ok"
                except:
                    order["deadline"] = "-"
                    order["days_left"] = 99
                    order["urgency"] = "ok"
            else:
                order["deadline"] = "-"
                order["days_left"] = 99
                order["urgency"] = "ok"

        return templates.TemplateResponse(name="proveedor.html", request=request, context={
            "supplier": supplier,
            "supplier_name": supplier_name,
            "pending": data["pending"],
            "completed": data["completed"],
        })
    except Exception as e:
        return templates.TemplateResponse(name="proveedor.html", request=request, context={
            "supplier": supplier,
            "supplier_name": "Barto" if supplier == "barto" else "Lola",
            "pending": [],
            "completed": [],
            "error": str(e),
        })


@app.post("/proveedor/barto/entregar/{order_id}")
def barto_mark_done(request: Request, order_id: int):
    try:
        mark_joya_terminada(order_id)
        return RedirectResponse(url="/proveedor/barto", status_code=302)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/bulk-deliver")
def bulk_deliver(request: Request):
    """Mark all orders before a given order number as delivered."""
    import datetime as _dt
    conn = get_db()
    now = _dt.datetime.now().isoformat()
    # Mark all orders with shopify_order_number < #1900 as entregado
    orders = conn.execute(
        "SELECT id, shopify_order_number FROM orders WHERE status != 'entregado'"
    ).fetchall()
    count = 0
    for o in orders:
        num_str = (o["shopify_order_number"] or "").replace("#", "").strip()
        try:
            num = int(num_str)
        except ValueError:
            continue
        if num < 1900:
            conn.execute(
                "UPDATE orders SET status='entregado', joya_terminada='1', joya_terminada_at=?, updated_at=? WHERE id=?",
                (now, now, o["id"])
            )
            count += 1
    conn.commit()
    conn.close()
    return JSONResponse({"ok": True, "delivered": count})


@app.post("/proveedor/lola/entregar/{order_id}")
def lola_mark_done(request: Request, order_id: int):
    try:
        mark_piedras_entregadas(order_id)
        return RedirectResponse(url="/proveedor/lola", status_code=302)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
@app.post("/api/sync")
def sync_orders():
    try:
        # Get current order IDs before sync to detect new ones
        conn = get_db()
        existing_ids = set(r["shopify_order_id"] for r in conn.execute("SELECT shopify_order_id FROM orders").fetchall())
        conn.close()

        count = sync_from_api(full=True)

        # Auto-notify disabled: María notifica manualmente desde cada pedido
        # _auto_notify_new_orders(existing_ids)

        return JSONResponse({"ok": True, "count": count, "source": "shopify_api"})
    except Exception as api_err:
        import traceback
        err_detail = traceback.format_exc()
        print(f"API sync failed: {err_detail}")
        try:
            count = sync_from_csv()
            return JSONResponse({"ok": True, "count": count, "source": "csv", "api_error": str(api_err)})
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e), "api_error": str(api_err)}, status_code=500)


def _auto_notify_new_orders(existing_ids_before_sync: set):
    """Auto-notify suppliers for orders that appeared after sync."""
    try:
        conn = get_db()
        new_orders = conn.execute(
            "SELECT * FROM orders WHERE auto_notified != '1' AND status='nuevo' AND COALESCE(product_type,'joya')='joya'"
        ).fetchall()
        conn.close()

        notified = 0
        for row in new_orders:
            order = dict(row)
            # Only notify if this is genuinely new (wasn't in DB before sync)
            if order.get("shopify_order_id") in existing_ids_before_sync:
                continue

            try:
                result = notify_supplier("barto", order)
                log_activity(order["id"], "Auto-notificación a Barto", result.get("email_subject", ""))

                if float(order.get("lola_estimado", 0) or 0) > 0:
                    result = notify_supplier("lola", order)
                    log_activity(order["id"], "Auto-notificación a Lola", result.get("email_subject", ""))

                update_order(order["id"], {
                    "auto_notified": "1",
                    "status": "notificado",
                    "barto_notified_at": datetime.now().isoformat(),
                })
                notified += 1
            except Exception as e:
                log_activity(order["id"], "Error en auto-notificación", str(e))
                print(f"Auto-notify failed for order {order['id']}: {e}")

        if notified > 0:
            print(f"Auto-notified suppliers for {notified} new orders")
    except Exception as e:
        print(f"Auto-notify batch failed: {e}")


@app.post("/api/orders/manual")
async def create_manual_order(request: Request):
    """Create a manual order (WhatsApp, presencial, etc.) with order number N/A."""
    try:
        body = await request.json()
        from datetime import date as d
        from catalog import estimate_costs

        product = body.get("product_name", "").strip()
        customer = body.get("customer_name", "").strip()
        pvp = float(body.get("pvp", 0))
        if not product or not customer or pvp <= 0:
            return JSONResponse({"ok": False, "error": "Faltan campos obligatorios"}, status_code=400)

        estimates = estimate_costs(product, pvp) or {}

        metodo_pago = body.get("metodo_pago", "transferencia")
        # Efectivo: always mark as paid (cash received at point of sale)
        estado_pago = body.get("estado_pago", "pendiente")
        if metodo_pago == "efectivo":
            estado_pago = "pagado"

        data = {
            "shopify_order_id": f"MANUAL-{int(__import__('time').time())}",
            "shopify_order_number": "N/A",
            "customer_name": customer,
            "customer_email": body.get("email", "").strip(),
            "customer_phone": body.get("phone", "").strip(),
            "customer_address": "",
            "product_name": product,
            "product_type": "joya",
            "ring_size": body.get("ring_size", "").strip(),
            "variant": "",
            "fecha_pedido": d.today().isoformat(),
            "pvp": pvp,
            "payment_gateway": metodo_pago,
            "estado_pago": estado_pago,
            "notes": body.get("notes", "").strip(),
            "status": "nuevo",
            **estimates,
        }
        from models import upsert_order
        order_id = upsert_order(data)
        log_activity(order_id, "Pedido manual creado", f"Vía: {body.get('via', 'whatsapp')}")
        return JSONResponse({"ok": True, "order_id": order_id})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/orders/test")
async def create_test_order(request: Request):
    """Create a test order for demo purposes."""
    try:
        body = await request.json()
        from datetime import date as d
        from catalog import estimate_costs
        product = body.get("product_name", "Aluna")
        size = body.get("ring_size", "")
        customer = body.get("customer_name", "Cliente Prueba")
        pvp = float(body.get("pvp", 0))

        estimates = estimate_costs(product, pvp, 160) or {}

        data = {
            "shopify_order_id": f"TEST-{int(__import__('time').time())}",
            "shopify_order_number": "#TEST",
            "customer_name": customer,
            "customer_email": body.get("email", ""),
            "customer_phone": body.get("phone", ""),
            "customer_address": "",
            "product_name": product,
            "product_type": "joya",
            "ring_size": size,
            "variant": body.get("variant", ""),
            "fecha_pedido": d.today().isoformat(),
            "pvp": pvp,
            "is_partial_payment": "0",
            "payment_group": "",
            "status": "nuevo",
            **estimates,
        }
        from models import upsert_order
        order_id = upsert_order(data)
        return JSONResponse({"ok": True, "order_id": order_id, "data": data})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/orders/test-cleanup")
def cleanup_test_orders():
    """Remove all test orders."""
    try:
        conn = get_db()
        conn.execute("DELETE FROM orders WHERE shopify_order_id LIKE 'TEST%'")
        conn.commit()
        conn.close()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/orders/{order_id}/notify-customer")
async def notify_customer_endpoint(order_id: str, request: Request):
    """Send a customer notification email using a template."""
    try:
        data = await request.json()
        template_key = data.get("template", "")
        fase = data.get("fase", "")

        order = get_order(order_id)
        if not order:
            return JSONResponse({"ok": False, "error": "Pedido no encontrado"}, status_code=404)

        result = notify_customer(order, template_key, fase)
        if result.get("ok"):
            log_activity(order_id, f"Email cliente: {template_key}", result.get("email_subject", ""))

        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/customer-templates")
def list_customer_templates():
    """List available customer email templates."""
    return JSONResponse({k: {"subject": v["subject"]} for k, v in CUSTOMER_TEMPLATES.items()})


@app.put("/api/orders/{order_id}")
async def update_order_endpoint(order_id: str, request: Request):
    try:
        data = await request.json()
        update_order(order_id, data)
        updated = get_order(order_id)
        export_order_to_excel(updated)
        return JSONResponse({"ok": True, "order": updated})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/orders/{order_id}/notify/{supplier}")
def notify_supplier_endpoint(order_id: str, supplier: str):
    try:
        order = get_order(order_id)
        if not order:
            return JSONResponse({"ok": False, "error": "Pedido no encontrado"}, status_code=404)

        result = notify_supplier(supplier, order)

        log_activity(order_id, f"Notificacion enviada a {supplier}", result.get("email_subject", ""))

        update_data = {"status": "notificado"}
        update_data[f"{supplier}_notified_at"] = datetime.now().isoformat()
        update_order(order_id, update_data)

        return JSONResponse({
            "ok": True,
            "whatsapp_link": result.get("whatsapp_link"),
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/orders/{order_id}/estado-pago")
async def change_estado_pago(order_id: str, request: Request):
    """Change payment status for an order (pendiente/pagado)."""
    try:
        data = await request.json()
        estado = data.get("estado_pago", "")
        if estado not in ("pendiente", "pagado"):
            return JSONResponse({"ok": False, "error": "Estado de pago no válido"}, status_code=400)
        update_order(order_id, {"estado_pago": estado})
        log_activity(order_id, f"Estado de pago: {estado}")
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/orders/{order_id}/status")
async def change_status(order_id: str, request: Request):
    try:
        VALID_STATUSES = {'nuevo', 'en_taller', 'recibido', 'enviado', 'archivado', 'cambio_talla', 'reparacion',
                          'notificado', 'entregado', 'completado'}  # legacy compat
        data = await request.json()
        new_status = data.get("status")
        if not new_status:
            return JSONResponse({"ok": False, "error": "Falta el campo status"}, status_code=400)
        if new_status not in VALID_STATUSES:
            return JSONResponse({"ok": False, "error": f"Estado no válido: {new_status}"}, status_code=400)

        update_data = {"status": new_status}

        # When marking as "recibido" (or legacy "entregado"), auto-capture today's real gold price
        if new_status in ("recibido", "entregado"):
            real_gold = get_current_gold_price()
            order = get_order(order_id)
            if order:
                peso_est = float(order.get("peso_estimado", 0) or 0)
                oro_real = peso_est * real_gold
                update_data["precio_oro_real"] = round(real_gold, 2)
                update_data["peso_real"] = peso_est  # pre-fill with estimate
                update_data["oro_total_real"] = round(oro_real, 2)
                log_activity(order_id, "Precio oro capturado al recibir de taller",
                             f"Oro 24K: {real_gold:.2f} EUR/gr x {peso_est:.1f}gr = {oro_real:.2f} EUR")

        STATUS_LABELS = {
            'nuevo': 'Nuevo', 'en_taller': 'En taller', 'recibido': 'Recibido de taller',
            'enviado': 'Enviado/Recogido', 'archivado': 'Archivado',
            'cambio_talla': 'Cambio de talla', 'reparacion': 'En reparación',
        }
        # Cambio de talla: save details + notify Barto
        whatsapp_link = None
        if new_status == "cambio_talla":
            nueva_talla = data.get("nueva_talla", "")
            talla_original = data.get("talla_original", "")
            update_data["cambio_talla_solicitado"] = "1"
            update_data["cambio_talla_solicitado_at"] = datetime.now().isoformat()
            update_data["cambio_talla_completado"] = "0"
            if nueva_talla:
                update_data["ring_size"] = nueva_talla
            order = get_order(order_id)
            if order:
                from notifications import whatsapp_cambio_talla, generate_whatsapp_link, SUPPLIERS
                import urllib.parse
                msg = whatsapp_cambio_talla(order, talla_original, nueva_talla)
                phone = SUPPLIERS["barto"]["phone"]
                whatsapp_link = f"https://wa.me/{phone}?text={urllib.parse.quote(msg)}"
                log_activity(order_id, "Cambio de talla solicitado",
                             f"Talla original: {talla_original} → Nueva: {nueva_talla}")

        # Reparación: save details + notify Barto
        if new_status == "reparacion":
            arreglo_desc = data.get("arreglo_descripcion", "")
            update_data["arreglo_solicitado"] = "1"
            update_data["arreglo_solicitado_at"] = datetime.now().isoformat()
            update_data["arreglo_completado"] = "0"
            update_data["arreglo_descripcion"] = arreglo_desc
            order = get_order(order_id)
            if order:
                from notifications import whatsapp_arreglo, SUPPLIERS
                import urllib.parse
                msg = whatsapp_arreglo(order, arreglo_desc)
                phone = SUPPLIERS["barto"]["phone"]
                whatsapp_link = f"https://wa.me/{phone}?text={urllib.parse.quote(msg)}"
                log_activity(order_id, "Arreglo solicitado", arreglo_desc)

        update_order(order_id, update_data)
        log_activity(order_id, f"Estado cambiado a {STATUS_LABELS.get(new_status, new_status)}")

        result = {"ok": True, "gold_price_captured": update_data.get("precio_oro_real")}
        if whatsapp_link:
            result["whatsapp_link"] = whatsapp_link
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/excel/export")
def export_excel():
    try:
        count = export_all_to_excel()
        return JSONResponse({"ok": True, "count": count})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/gold-price")
def gold_price_endpoint():
    try:
        return JSONResponse(get_gold_info())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/settings/gold-price")
async def update_gold_price(request: Request):
    try:
        data = await request.json()
        price = data.get("price")
        if price is None:
            return JSONResponse({"ok": False, "error": "Falta el campo price"}, status_code=400)

        set_setting("gold_price", float(price))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/orders/{order_id}/update-costs")
async def update_real_costs(order_id: str, request: Request):
    try:
        data = await request.json()
        order = get_order(order_id)
        if not order:
            return JSONResponse({"ok": False, "error": "Pedido no encontrado"}, status_code=404)

        lola_real = float(data.get("lola_real", 0))
        barto_real = float(data.get("barto_real", 0))
        peso_real = float(data.get("peso_real", 0))
        precio_oro_real = float(data.get("precio_oro_real", 0))
        envio_packaging = float(data.get("envio_packaging", 0))

        oro_total_real = peso_real * precio_oro_real
        cmv_real = lola_real + barto_real + oro_total_real + envio_packaging

        base_imponible = float(order.get("base_imponible", 0) or 0)
        comision = float(order.get("comision", 0) or 0)
        beneficio_bruto_real = base_imponible - comision - cmv_real

        cost_data = {
            "lola_real": lola_real,
            "barto_real": barto_real,
            "peso_real": peso_real,
            "precio_oro_real": precio_oro_real,
            "envio_packaging": envio_packaging,
            "oro_total_real": oro_total_real,
            "cmv_real": cmv_real,
            "beneficio_bruto_real": beneficio_bruto_real,
        }

        update_order(order_id, cost_data)
        updated_order = get_order(order_id)
        export_order_to_excel(updated_order)
        log_activity(order_id, "Costes reales actualizados")

        return JSONResponse({"ok": True, "order": updated_order})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/orders/{order_id}/cambio-talla")
async def request_size_change(order_id: int, request: Request):
    import urllib.parse as _up
    data = await request.json()
    talla_original = data.get("talla_original", "")
    talla_nueva = data.get("talla_nueva", "")
    update_order(order_id, {
        "cambio_talla_original": talla_original,
        "cambio_talla_nueva": talla_nueva,
        "cambio_talla_solicitado": "1",
        "cambio_talla_solicitado_at": datetime.now().isoformat(),
    })
    log_activity(order_id, "Cambio de talla solicitado", f"De {talla_original} a {talla_nueva}")
    order = get_order(order_id)
    from notifications import whatsapp_cambio_talla
    msg = whatsapp_cambio_talla(order, talla_original, talla_nueva)
    wa_link = f"https://wa.me/34659319904?text={_up.quote(msg)}"
    return JSONResponse({"ok": True, "whatsapp_link": wa_link})


@app.post("/api/orders/{order_id}/arreglo")
async def request_repair(order_id: int, request: Request):
    import urllib.parse as _up
    data = await request.json()
    descripcion = data.get("descripcion", "")
    update_order(order_id, {
        "arreglo_descripcion": descripcion,
        "arreglo_solicitado": "1",
        "arreglo_solicitado_at": datetime.now().isoformat(),
    })
    log_activity(order_id, "Arreglo solicitado", descripcion)
    order = get_order(order_id)
    from notifications import whatsapp_arreglo
    msg = whatsapp_arreglo(order, descripcion)
    wa_link = f"https://wa.me/34659319904?text={_up.quote(msg)}"
    return JSONResponse({"ok": True, "whatsapp_link": wa_link})


@app.post("/proveedor/barto/cambio-completado/{order_id}")
def barto_cambio_done(request: Request, order_id: int):
    update_order(order_id, {"cambio_talla_completado": "1"})
    log_activity(order_id, "Cambio de talla completado")
    return RedirectResponse(url="/proveedor/barto", status_code=302)


@app.post("/proveedor/barto/arreglo-completado/{order_id}")
def barto_arreglo_done(request: Request, order_id: int):
    update_order(order_id, {"arreglo_completado": "1"})
    log_activity(order_id, "Arreglo completado")
    return RedirectResponse(url="/proveedor/barto", status_code=302)


@app.post("/api/fix-all-status")
def fix_all_status():
    """Fix status: <#1900 entregado, >=#1900 nuevo (except joyeros→entregado)."""
    import datetime as _dt
    conn = get_db()
    now = _dt.datetime.now().isoformat()

    orders = conn.execute("SELECT id, shopify_order_number, product_type, joya_terminada FROM orders").fetchall()
    for o in orders:
        num_str = (o["shopify_order_number"] or "").replace("#", "").strip()
        try:
            num = int(num_str)
        except ValueError:
            continue

        if (o["product_type"] or "") == "joyero":
            new_status = "archivado"
        elif o["joya_terminada"] == "1":
            new_status = "recibido"
        elif num < 1900:
            new_status = "archivado"
        else:
            new_status = "nuevo"

        conn.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?", (new_status, now, o["id"]))

    conn.commit()

    stats = {}
    for s in ['nuevo', 'en_taller', 'recibido', 'enviado', 'archivado', 'cambio_talla', 'reparacion']:
        stats[s] = conn.execute("SELECT COUNT(*) as c FROM orders WHERE status=?", (s,)).fetchone()["c"]

    conn.close()
    return JSONResponse({"ok": True, "status_counts": stats})

@app.post("/api/set-status-range")
def set_status_range(request: Request):
    """Set status for a range of order numbers. Body: {from: 1900, to: 1908, status: 'notificado'}"""
    import json, datetime as _dt
    body = json.loads(request._receive.__self__._body.decode() if hasattr(request, '_receive') else '{}')
    # Simpler: use query params
    return JSONResponse({"error": "Use query params: ?from=1900&to=1908&status=notificado"})

@app.get("/api/set-status-range")
def set_status_range_get(request: Request, from_num: int = 0, to_num: int = 0, status: str = "notificado"):
    """Set status for a range. GET /api/set-status-range?from_num=1900&to_num=1908&status=notificado"""
    import datetime as _dt
    if not from_num or not to_num or status not in ("nuevo", "notificado", "entregado", "completado"):
        return JSONResponse({"error": "Params: from_num, to_num, status"}, status_code=400)
    conn = get_db()
    now = _dt.datetime.now().isoformat()
    orders = conn.execute("SELECT id, shopify_order_number, product_type FROM orders").fetchall()
    count = 0
    for o in orders:
        num_str = (o["shopify_order_number"] or "").replace("#", "").strip()
        try:
            num = int(num_str)
        except ValueError:
            continue
        if from_num <= num <= to_num and (o["product_type"] or "joya") != "joyero":
            conn.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?", (status, now, o["id"]))
            count += 1
    conn.commit()
    conn.close()
    return JSONResponse({"ok": True, "updated": count, "status": status, "range": f"#{from_num}-#{to_num}"})

@app.post("/api/bank/reset")
def reset_bank_entries():
    """Delete all bank entries so we can re-import with corrected categorization."""
    conn = get_db()
    conn.execute("DELETE FROM bank_entries")
    conn.commit()
    count = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    return JSONResponse({"ok": True, "deleted": count})

# ---------------------------------------------------------------------------
# Leads (CRM)
# ---------------------------------------------------------------------------
@app.get("/leads", dependencies=[Depends(require_auth)])
def leads_page(request: Request, estado: str = None, via: str = None, search: str = None):
    try:
        leads = get_all_leads(estado=estado, via=via, search=search)
        stats = get_lead_stats()
        return templates.TemplateResponse(name="leads.html", request=request, context={
            "leads": leads,
            "stats": stats,
            "current_estado": estado or "",
            "current_via": via or "",
            "current_search": search or "",
        })
    except Exception as e:
        return templates.TemplateResponse(name="leads.html", request=request, context={
            "leads": [],
            "stats": {},
            "current_estado": "",
            "current_via": "",
            "current_search": "",
            "error": str(e),
        })


@app.get("/data-protection")
def contacto_page(request: Request, success: str = None):
    """Public contact form (embeddable in Shopify via iframe)."""
    return templates.TemplateResponse("contacto_embed.html", {
        "request": request, "success": success == "1"
    })


@app.post("/data-protection")
async def contacto_submit(request: Request):
    """Handle contact form submission."""
    try:
        form = await request.form()
        nombre = form.get("nombre", "").strip()
        apellido = form.get("apellido", "").strip()
        full_name = f"{nombre} {apellido}".strip()
        lead_data = {
            "nombre": full_name,
            "telefono": form.get("telefono", "").strip(),
            "email": form.get("email", "").strip(),
            "via_contacto": "web",
            "tipo": "asesoramiento",
            "notas": f"LOPD: si, Comercial: {'si' if form.get('comercial') else 'no'}",
        }
        insert_lead(lead_data)
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/data-protection?success=1", status_code=302)
    except Exception as e:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/contacto?error=1", status_code=302)


@app.post("/api/leads/web")
async def create_lead_web(request: Request):
    """Public endpoint for web contact form (no auth). Accepts form data."""
    try:
        form = await request.form()
        nombre = form.get("nombre", "").strip()
        apellido = form.get("apellido", "").strip()
        full_name = f"{nombre} {apellido}".strip()
        lead_data = {
            "nombre": full_name,
            "telefono": form.get("telefono", "").strip(),
            "email": form.get("email", "").strip(),
            "via_contacto": form.get("via", "web"),
            "tipo": "asesoramiento",
            "notas": f"LOPD: {'si' if form.get('lopd') else 'no'}, Comercial: {'si' if form.get('comercial') else 'no'}",
        }
        lead_id = insert_lead(lead_data)
        log_activity(None, "Nuevo lead web", f"Lead #{lead_id}: {full_name} ({lead_data['email']})")
        redirect_url = form.get("redirect", "https://antiqua.store")
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=redirect_url, status_code=302)
    except Exception as e:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="https://antiqua.store?error=1", status_code=302)


@app.post("/api/leads", dependencies=[Depends(require_auth)])
async def create_lead(request: Request):
    try:
        data = await request.json()
        lead_data = {
            "nombre": data.get("nombre", "").strip(),
            "telefono": data.get("telefono", "").strip(),
            "email": data.get("email", "").strip(),
            "via_contacto": data.get("via_contacto", "otro"),
            "tipo": data.get("tipo", "asesoramiento"),
            "fecha_cita": data.get("fecha_cita", ""),
            "hora_cita": data.get("hora_cita", ""),
            "notas": data.get("notas", ""),
            "estado": data.get("estado", "nuevo"),
        }
        lead_id = insert_lead(lead_data)
        return JSONResponse({"ok": True, "lead_id": lead_id})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/leads/{lead_id}")
async def update_lead_endpoint(lead_id: int, request: Request):
    try:
        data = await request.json()
        update_lead(lead_id, data)
        updated = get_lead(lead_id)
        return JSONResponse({"ok": True, "lead": updated})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/leads/{lead_id}/link-order")
async def link_lead_to_order(lead_id: int, request: Request):
    """Manually link a lead to a Shopify order."""
    try:
        data = await request.json()
        shopify_order_id = data.get("shopify_order_id", "")
        if not shopify_order_id:
            return JSONResponse({"ok": False, "error": "Falta shopify_order_id"}, status_code=400)
        convert_lead(lead_id, shopify_order_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/leads/stats")
def leads_stats_endpoint():
    try:
        return JSONResponse(get_lead_stats())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Webhooks (Calendly, Shopify)
# ---------------------------------------------------------------------------
@app.post("/api/webhooks/calendly")
async def calendly_webhook(request: Request):
    """Receive Calendly invitee.created events and auto-create leads."""
    try:
        body = await request.json()
        event_type = body.get("event", "")
        if event_type != "invitee.created":
            return JSONResponse({"ok": True, "skipped": True})

        payload = body.get("payload", {})
        invitee = payload.get("invitee", payload)
        name = invitee.get("name", "") or payload.get("name", "")
        email = invitee.get("email", "") or payload.get("email", "")

        # Extract event time
        event_info = payload.get("event", {}) or payload.get("scheduled_event", {})
        start_time = ""
        if isinstance(event_info, dict):
            start_time = event_info.get("start_time", "") or event_info.get("start", "")

        fecha_cita = ""
        hora_cita = ""
        if start_time and len(start_time) >= 16:
            fecha_cita = start_time[:10]
            hora_cita = start_time[11:16]

        calendly_event_id = payload.get("uri", "") or payload.get("event_uri", "")

        lead_data = {
            "nombre": name,
            "email": email,
            "telefono": "",
            "via_contacto": "calendly",
            "tipo": "cita_showroom",
            "estado": "cita_reservada",
            "fecha_cita": fecha_cita,
            "hora_cita": hora_cita,
            "calendly_event_id": calendly_event_id,
            "notas": f"Reserva automática desde Calendly",
        }
        lead_id = insert_lead(lead_data)
        return JSONResponse({"ok": True, "lead_id": lead_id})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/webhooks/shopify/order-created")
async def shopify_order_webhook(request: Request):
    """Receive Shopify order/create webhooks for real-time processing."""
    try:
        body = await request.json()
        # Sync this specific order
        from shopify_client import process_webhook_order
        order_ids = process_webhook_order(body)

        # Auto-match leads and auto-notify suppliers
        for oid in order_ids:
            order = get_order(oid)
            if not order:
                continue

            # Auto-match lead
            lead_id = match_lead_to_order(
                order.get("customer_email", ""),
                order.get("customer_phone", ""),
                order.get("customer_name", "")
            )
            if lead_id:
                convert_lead(lead_id, order.get("shopify_order_id", ""))
                update_order(oid, {"lead_id": lead_id})
                log_activity(oid, "Lead convertido automáticamente", f"Lead #{lead_id}")

            # Auto-notify disabled: María notifica manualmente desde cada pedido
            # Cuando quieran activarlo, descomentar este bloque

        return JSONResponse({"ok": True, "processed": len(order_ids)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Production Phases (Barto portal)
# ---------------------------------------------------------------------------
@app.post("/proveedor/barto/fase/{order_id}")
async def barto_update_phase(request: Request, order_id: int):
    """Barto updates production phase for an order."""
    try:
        form = await request.form()
        new_phase = form.get("fase", "")
        advance_production_phase(order_id, new_phase)

        # If phase is 'terminado', also mark joya as finished
        if new_phase == "terminado":
            mark_joya_terminada(order_id)

        return RedirectResponse(url="/proveedor/barto", status_code=302)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Contabilidad (Accounting)
# ---------------------------------------------------------------------------
@app.get("/contabilidad", dependencies=[Depends(require_auth)])
def contabilidad_page(request: Request, from_month: str = None, to_month: str = None, month: str = None):
    try:
        # Support old ?month= param and new ?from_month=&to_month= params
        if month and not from_month:
            from_month = month
            to_month = month
        if not from_month:
            from_month = datetime.now().strftime("%Y-%m")
        if not to_month:
            to_month = from_month

        acc_stats = get_accounting_stats(from_month=from_month, to_month=to_month)
        entries = get_bank_entries(from_month=from_month, to_month=to_month)
        cash = get_cash_sales(from_month=from_month, to_month=to_month)
        gold_price_info = get_gold_info()
        unmatched = get_unmatched_summary(entries)
        return templates.TemplateResponse(name="contabilidad.html", request=request, context={
            "stats": acc_stats,
            "entries": entries,
            "cash_sales": cash,
            "gold_price": gold_price_info,
            "from_month": from_month,
            "to_month": to_month,
            "unmatched": unmatched,
        })
    except Exception as e:
        now = datetime.now().strftime("%Y-%m")
        return templates.TemplateResponse(name="contabilidad.html", request=request, context={
            "stats": {},
            "entries": [],
            "cash_sales": [],
            "gold_price": get_gold_info(),
            "from_month": from_month or now,
            "to_month": to_month or now,
            "error": str(e),
        })


@app.post("/api/bank/upload", dependencies=[Depends(require_auth)])
async def upload_bank_csv(request: Request, file: UploadFile = File(...)):
    try:
        content = await file.read()
        parsed = parse_bank_csv(content)

        # Categorize each entry
        for entry in parsed:
            entry["categoria"] = categorize_entry(entry.get("concepto", ""), entry.get("importe", 0))

        # Match with existing orders
        orders = get_all_orders()
        parsed = match_with_orders(parsed, orders)

        # Insert into DB
        count = 0
        for entry in parsed:
            insert_bank_entry(entry)
            count += 1

        # Get updated stats for the month of the first entry
        month = None
        if parsed:
            month = parsed[0].get("fecha", "")[:7]
        acc_stats = get_accounting_stats(month)

        return JSONResponse({"ok": True, "count": count, "stats": acc_stats})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/bank/{entry_id}/categorize", dependencies=[Depends(require_auth)])
async def categorize_bank_entry(request: Request, entry_id: int):
    try:
        data = await request.json()
        categoria = data.get("categoria", "")
        notas = data.get("notas")
        update_data = {"categoria": categoria}
        if notas is not None:
            update_data["notas"] = notas
        update_bank_entry(entry_id, update_data)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/cash-sales", dependencies=[Depends(require_auth)])
async def add_cash_sale(request: Request):
    try:
        form = await request.form()
        data = {
            "fecha": form.get("fecha", ""),
            "cliente": form.get("cliente", ""),
            "producto": form.get("producto", ""),
            "importe": float(form.get("importe", 0)),
            "notas": form.get("notas", ""),
        }
        insert_cash_sale(data)
        month = data["fecha"][:7] if data["fecha"] else ""
        return RedirectResponse(f"/contabilidad?month={month}", status_code=302)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/cash-sales/{sale_id}", dependencies=[Depends(require_auth)])
def delete_cash_sale_route(request: Request, sale_id: int):
    try:
        delete_cash_sale(sale_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
