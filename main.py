import os, secrets
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException, status
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
)
from catalog import load_catalog, estimate_costs
from shopify_client import sync_from_api, sync_from_csv
from excel_sync import export_order_to_excel, import_excel_to_db, export_all_to_excel
from notifications import notify_supplier, get_notification_preview, SUPPLIERS
from gold_price import get_gold_info, get_18k_gold_price, get_current_gold_price

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = int(os.getenv("PORT", 8000))
DEFAULT_GOLD_PRICE = float(os.getenv("GOLD_PRICE_PER_GRAM", 92.0))

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

# ---------------------------------------------------------------------------
# HTML Pages
# ---------------------------------------------------------------------------
@app.get("/", dependencies=[Depends(require_auth)])
def dashboard(request: Request, status: str = None, month: str = None, search: str = None):
    try:
        filters = {}
        if status:
            filters["status"] = status
        if month:
            filters["month"] = month
        if search:
            filters["search"] = search

        orders = get_all_orders(**filters)
        stats = get_dashboard_stats()

        gold_price_info = get_gold_info()

        return templates.TemplateResponse(name="dashboard.html", request=request, context={
            "orders": orders,
            "stats": stats,
            "gold_price": gold_price_info,
            "current_status": status or "",
            "current_month": month or "",
            "current_search": search or "",
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
        return templates.TemplateResponse(name="order.html", request=request, context={
            "order": None,
            "activity_log": [],
            "notification_preview": None,
            "catalog_match": None,
            "error": str(e),
        })


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
        count = sync_from_api(full=True)
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


@app.post("/api/orders/{order_id}/status")
async def change_status(order_id: str, request: Request):
    try:
        data = await request.json()
        new_status = data.get("status")
        if not new_status:
            return JSONResponse({"ok": False, "error": "Falta el campo status"}, status_code=400)

        update_data = {"status": new_status}

        # When marking as "entregado", auto-capture today's real gold price
        if new_status == "entregado":
            real_gold = get_current_gold_price()
            order = get_order(order_id)
            if order:
                peso_est = float(order.get("peso_estimado", 0) or 0)
                oro_real = peso_est * real_gold
                update_data["precio_oro_real"] = round(real_gold, 2)
                update_data["peso_real"] = peso_est  # pre-fill with estimate
                update_data["oro_total_real"] = round(oro_real, 2)
                log_activity(order_id, "Precio oro capturado al entregar",
                             f"Oro 24K: {real_gold:.2f} EUR/gr x {peso_est:.1f}gr = {oro_real:.2f} EUR")

        update_order(order_id, update_data)
        log_activity(order_id, f"Estado cambiado a {new_status}")

        return JSONResponse({"ok": True, "gold_price_captured": update_data.get("precio_oro_real")})
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
            new_status = "entregado"
        elif o["joya_terminada"] == "1":
            new_status = "entregado"
        elif num < 1900:
            new_status = "entregado"
        else:
            new_status = "nuevo"

        conn.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?", (new_status, now, o["id"]))

    conn.commit()

    stats = {}
    for s in ['nuevo', 'notificado', 'entregado', 'completado']:
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

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
