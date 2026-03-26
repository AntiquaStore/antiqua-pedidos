import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from jinja2 import Environment, FileSystemLoader
from fastapi.templating import Jinja2Templates

from models import (
    init_db, get_db, get_order, get_all_orders, update_order,
    log_activity, get_activity, get_dashboard_stats,
    get_supplier_summary, get_setting, set_setting, get_all_products,
)
from catalog import load_catalog, estimate_costs
from shopify_client import sync_from_api, sync_from_csv
from excel_sync import export_order_to_excel, import_excel_to_db, export_all_to_excel
from notifications import notify_supplier, get_notification_preview, SUPPLIERS

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = int(os.getenv("PORT", 8000))
DEFAULT_GOLD_PRICE = float(os.getenv("GOLD_PRICE_PER_GRAM", 92.0))

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Antiqua - Gestión de Pedidos")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
def on_startup():
    init_db()
    load_catalog()

# ---------------------------------------------------------------------------
# HTML Pages
# ---------------------------------------------------------------------------
@app.get("/")
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

        gold_price = get_setting("gold_price")
        if gold_price is None:
            gold_price = DEFAULT_GOLD_PRICE

        return templates.TemplateResponse(name="dashboard.html", request=request, context={
            "orders": orders,
            "stats": stats,
            "gold_price": gold_price,
            "current_status": status or "",
            "current_month": month or "",
            "current_search": search or "",
        })
    except Exception as e:
        return templates.TemplateResponse(name="dashboard.html", request=request, context={
            "orders": [],
            "stats": {},
            "gold_price": DEFAULT_GOLD_PRICE,
            "current_status": "",
            "current_month": "",
            "current_search": "",
            "error": str(e),
        })


@app.get("/order/{order_id}")
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


@app.get("/products")
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


@app.get("/suppliers")
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
# API Endpoints
# ---------------------------------------------------------------------------
@app.post("/api/sync")
def sync_orders():
    try:
        count = sync_from_api()
        return JSONResponse({"ok": True, "count": count})
    except Exception:
        try:
            count = sync_from_csv()
            return JSONResponse({"ok": True, "count": count, "source": "csv"})
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

        update_order(order_id, {"status": new_status})
        log_activity(order_id, f"Estado cambiado a {new_status}")

        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/excel/export")
def export_excel():
    try:
        count = export_all_to_excel()
        return JSONResponse({"ok": True, "count": count})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


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

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
