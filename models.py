"""
SQLite database models for Antiqua order management.
"""
import sqlite3, os, datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "antiqua.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS products (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT UNIQUE NOT NULL,
        tipo        TEXT,
        piedras_desc TEXT,
        piedras_total REAL DEFAULT 0,
        diamantes_desc TEXT,
        diamantes_total REAL DEFAULT 0,
        otros_desc  TEXT,
        otros_total REAL DEFAULT 0,
        taller_hechura REAL DEFAULT 0,
        taller_engaste REAL DEFAULT 0,
        taller_otros REAL DEFAULT 0,
        taller_total REAL DEFAULT 0,
        peso_gr     REAL DEFAULT 0,
        oro_precio_gr REAL DEFAULT 0,
        oro_total   REAL DEFAULT 0,
        cmv         REAL DEFAULT 0,
        envio       REAL DEFAULT 0,
        pvp         REAL DEFAULT 0,
        iva         REAL DEFAULT 0,
        ingreso     REAL DEFAULT 0,
        beneficio_bruto REAL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS orders (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        shopify_order_id        TEXT,
        shopify_order_number    TEXT,
        customer_name           TEXT,
        customer_email          TEXT,
        customer_phone          TEXT,
        customer_address        TEXT,
        product_name            TEXT,
        product_type            TEXT,
        ring_size               TEXT,
        variant                 TEXT,
        n_joya                  INTEGER,
        fecha_pedido            TEXT,
        pvp                     REAL DEFAULT 0,
        iva                     REAL DEFAULT 0,
        base_imponible          REAL DEFAULT 0,
        fecha_ingreso           TEXT,
        comision                REAL DEFAULT 0,
        ingreso_total           REAL DEFAULT 0,
        lola_estimado           REAL DEFAULT 0,
        lola_real               REAL,
        fecha_cobro_lola        TEXT,
        barto_estimado          REAL DEFAULT 0,
        barto_real              REAL,
        fecha_cobro_barto       TEXT,
        peso_estimado           REAL DEFAULT 0,
        peso_real               REAL,
        precio_oro_estimado     REAL DEFAULT 0,
        precio_oro_real         REAL,
        oro_total_estimado      REAL DEFAULT 0,
        oro_total_real          REAL,
        cmv_estimado            REAL DEFAULT 0,
        cmv_real                REAL,
        envio_packaging         REAL DEFAULT 0,
        beneficio_bruto_estimado REAL DEFAULT 0,
        beneficio_bruto_real    REAL,
        status                  TEXT DEFAULT 'nuevo',
        lola_notified_at        TEXT,
        barto_notified_at       TEXT,
        notes                   TEXT,
        created_at              TEXT,
        updated_at              TEXT,
        UNIQUE(shopify_order_id, product_name)
    );

    CREATE TABLE IF NOT EXISTS activity_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id    INTEGER REFERENCES orders(id),
        action      TEXT NOT NULL,
        details     TEXT,
        created_at  TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    # Add new columns if they don't exist (migration for existing DBs)
    for col, coltype in [
        ("is_partial_payment", "TEXT DEFAULT '0'"),
        ("payment_group", "TEXT"),
        ("piedras_desc", "TEXT"),
        ("diamantes_desc", "TEXT"),
        ("piedras_entregadas", "TEXT DEFAULT '0'"),
        ("piedras_entregadas_at", "TEXT"),
        ("joya_terminada", "TEXT DEFAULT '0'"),
        ("joya_terminada_at", "TEXT"),
        ("fecha_entrega_custom", "TEXT"),
        ("urgente", "TEXT DEFAULT '0'"),
        ("fase_produccion", "TEXT DEFAULT 'pendiente'"),
        ("fase_updated_at", "TEXT"),
        ("auto_notified", "TEXT DEFAULT '0'"),
        ("lead_id", "INTEGER"),
    ]:
        try:
            conn.execute(f"ALTER TABLE orders ADD COLUMN {col} {coltype}")
        except Exception:
            pass  # Column already exists

    # Migrate: unify en_taller → notificado (single workshop status)
    conn.execute("UPDATE orders SET status='notificado' WHERE status='en_taller'")

    # ── Accounting tables ──
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS bank_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concepto TEXT,
        fecha TEXT,
        importe REAL,
        saldo REAL,
        tipo TEXT DEFAULT 'ingreso',
        categoria TEXT,
        matched_order_id INTEGER,
        matched_invoice_id INTEGER,
        notas TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proveedor TEXT,
        fecha TEXT,
        total REAL,
        archivo_path TEXT,
        concepto TEXT,
        estado TEXT DEFAULT 'pendiente',
        notas TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS invoice_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER REFERENCES invoices(id),
        order_id INTEGER,
        descripcion TEXT,
        importe REAL,
        matched INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS cash_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT,
        cliente TEXT,
        producto TEXT,
        importe REAL,
        notas TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        telefono TEXT,
        email TEXT,
        via_contacto TEXT DEFAULT 'otro',
        tipo TEXT DEFAULT 'asesoramiento',
        fecha_cita TEXT,
        hora_cita TEXT,
        estado TEXT DEFAULT 'nuevo',
        notas TEXT,
        shopify_order_id TEXT,
        converted_at TEXT,
        calendly_event_id TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT
    );
    """)

    conn.commit()
    conn.close()


# ── Helper: dict from Row ──
def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    return [dict(r) for r in rows]


# ── Orders CRUD ──
def upsert_order(data: dict) -> int:
    conn = get_db()
    now = datetime.datetime.now().isoformat()
    data["updated_at"] = now
    if "created_at" not in data or not data.get("created_at"):
        data["created_at"] = now

    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    # Don't overwrite status, joya_terminada, piedras_entregadas on re-sync (preserve manual state changes)
    preserve_fields = ("shopify_order_id", "product_name", "created_at", "status", "joya_terminada", "joya_terminada_at", "piedras_entregadas", "piedras_entregadas_at")
    updates = ", ".join([f"{k}=excluded.{k}" for k in data if k not in preserve_fields])

    sql = f"""
    INSERT INTO orders ({cols}) VALUES ({placeholders})
    ON CONFLICT(shopify_order_id, product_name)
    DO UPDATE SET {updates}
    """
    cur = conn.execute(sql, list(data.values()))
    conn.commit()
    order_id = cur.lastrowid
    conn.close()
    return order_id


def get_order(order_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


def get_all_orders(status=None, month=None, search=None):
    conn = get_db()
    sql = "SELECT * FROM orders WHERE 1=1"
    params = []
    if status and status != "todos":
        sql += " AND status=?"
        params.append(status)
    if month:
        sql += " AND fecha_pedido LIKE ?"
        params.append(f"{month}%")
    if search:
        sql += " AND (product_name LIKE ? OR customer_name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    sql += " ORDER BY shopify_order_number DESC, fecha_pedido DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return merge_partial_payments(rows_to_list(rows))


def merge_partial_payments(orders: list) -> list:
    """Merge split payment orders into single rows.
    Orders with the same payment_group get combined into one entry."""
    groups = {}
    merged = []
    for o in orders:
        pg = o.get("payment_group", "")
        if pg:
            if pg not in groups:
                groups[pg] = {**o}
                # Clean up the product name (remove "first/second payment" etc)
                from shopify_client import clean_payment_name
                raw = o.get("product_name", "")
                groups[pg]["product_name"] = clean_payment_name(raw) or raw
                groups[pg]["_merged_orders"] = [o.get("shopify_order_number", "")]
                groups[pg]["_payment_count"] = 1
                merged.append(groups[pg])
            else:
                # Merge: sum PVP, combine order numbers
                groups[pg]["pvp"] = (groups[pg].get("pvp", 0) or 0) + (o.get("pvp", 0) or 0)
                groups[pg]["_merged_orders"].append(o.get("shopify_order_number", ""))
                groups[pg]["_payment_count"] = groups[pg].get("_payment_count", 1) + 1
                # Use earliest date
                if o.get("fecha_pedido", "") < groups[pg].get("fecha_pedido", ""):
                    groups[pg]["fecha_pedido"] = o["fecha_pedido"]
                # Combine order numbers for display
                groups[pg]["shopify_order_number"] = "/".join(groups[pg]["_merged_orders"])
        else:
            merged.append(o)
    return merged


def update_order(order_id: int, data: dict):
    conn = get_db()
    data["updated_at"] = datetime.datetime.now().isoformat()
    sets = ", ".join([f"{k}=?" for k in data])
    conn.execute(f"UPDATE orders SET {sets} WHERE id=?", list(data.values()) + [order_id])
    conn.commit()
    conn.close()


def log_activity(order_id: int, action: str, details: str = ""):
    conn = get_db()
    conn.execute("INSERT INTO activity_log (order_id, action, details) VALUES (?,?,?)",
                 (order_id, action, details))
    conn.commit()
    conn.close()


def get_activity(order_id: int):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM activity_log WHERE order_id=? ORDER BY created_at DESC",
        (order_id,)
    ).fetchall()
    conn.close()
    return rows_to_list(rows)


# ── Products ──
def upsert_product(data: dict):
    conn = get_db()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    updates = ", ".join([f"{k}=excluded.{k}" for k in data if k != "name"])
    sql = f"""
    INSERT INTO products ({cols}) VALUES ({placeholders})
    ON CONFLICT(name) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))
    conn.commit()
    conn.close()


def get_all_products():
    conn = get_db()
    rows = conn.execute("SELECT * FROM products ORDER BY name").fetchall()
    conn.close()
    return rows_to_list(rows)


def get_product_by_name(name: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE LOWER(name)=LOWER(?)", (name,)).fetchone()
    conn.close()
    return row_to_dict(row)


# ── Settings ──
def get_setting(key: str, default=None):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()


# ── Stats ──
def get_dashboard_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM orders").fetchone()["c"]
    pending = conn.execute("SELECT COUNT(*) as c FROM orders WHERE status != 'entregado'").fetchone()["c"]
    in_workshop = conn.execute("SELECT COUNT(*) as c FROM orders WHERE status='notificado'").fetchone()["c"]
    revenue = conn.execute("SELECT COALESCE(SUM(pvp / 1.21),0) as s FROM orders").fetchone()["s"]

    # Joyas vs Joyeros vs Cadenas counts (partial payments grouped = 1 joya)
    joyas_grouped = conn.execute(
        "SELECT COUNT(DISTINCT payment_group) as c FROM orders WHERE COALESCE(product_type,'joya') = 'joya' AND payment_group IS NOT NULL AND payment_group != ''"
    ).fetchone()["c"]
    joyas_ungrouped = conn.execute(
        "SELECT COUNT(*) as c FROM orders WHERE COALESCE(product_type,'joya') = 'joya' AND (payment_group IS NULL OR payment_group = '')"
    ).fetchone()["c"]
    joyas_count = joyas_grouped + joyas_ungrouped
    # Joyeros vendidos: solo los que se compraron solos (no como regalo con joyas >= 2500)
    # Un Relique Box es regalo si su shopify_order_id tiene otros items que no son joyero
    joyeros_count = conn.execute("""
        SELECT COUNT(*) as c FROM orders o1
        WHERE o1.product_type = 'joyero'
        AND NOT EXISTS (
            SELECT 1 FROM orders o2
            WHERE o2.shopify_order_id = o1.shopify_order_id
            AND o2.product_type != 'joyero'
        )
    """).fetchone()["c"]
    cadenas_count = conn.execute(
        "SELECT COUNT(*) as c FROM orders WHERE product_type = 'cadena'"
    ).fetchone()["c"]

    # Unique tickets & avg ticket (joyas only, merging split payments)
    # Orders with payment_group: group them (sum pvp, count as 1 ticket)
    # Orders without payment_group: each is 1 ticket
    # Exclude joyeros from ticket calculations
    grouped_tickets = conn.execute("""
        SELECT SUM(pvp / 1.21) as ticket_pvp
        FROM orders
        WHERE payment_group IS NOT NULL AND payment_group != ''
          AND COALESCE(product_type,'joya') = 'joya'
        GROUP BY payment_group
    """).fetchall()

    ungrouped_tickets = conn.execute("""
        SELECT pvp as ticket_pvp
        FROM orders
        WHERE (payment_group IS NULL OR payment_group = '')
          AND COALESCE(product_type,'joya') = 'joya'
          AND pvp > 0
    """).fetchall()

    all_tickets = [row["ticket_pvp"] for row in grouped_tickets if row["ticket_pvp"]] + \
                  [row["ticket_pvp"] for row in ungrouped_tickets if row["ticket_pvp"]]

    unique_tickets = len(all_tickets)
    total_joyas_revenue = sum(all_tickets) if all_tickets else 0
    avg_ticket_joyas = total_joyas_revenue / unique_tickets if unique_tickets > 0 else 0

    # Legacy avg_ticket (simple average of all orders with pvp > 0)
    avg_ticket = conn.execute(
        "SELECT COALESCE(AVG(pvp),0) as a FROM orders WHERE pvp > 0"
    ).fetchone()["a"]

    # Ticket medio con IVA (joyas only, same logic as avg_ticket_joyas but with IVA)
    avg_ticket_joyas_iva = avg_ticket_joyas * 1.21 if avg_ticket_joyas else 0

    # Ventas del mes actual y del año
    import datetime as _dt
    now = _dt.date.today()
    current_month = now.strftime("%Y-%m")
    current_year = now.strftime("%Y")
    ventas_mes = conn.execute(
        "SELECT COUNT(*) as c FROM orders WHERE fecha_pedido LIKE ? AND COALESCE(product_type,'joya') != 'joyero'",
        (f"{current_month}%",)
    ).fetchone()["c"]
    facturacion_mes = conn.execute(
        "SELECT COALESCE(SUM(pvp / 1.21),0) as s FROM orders WHERE fecha_pedido LIKE ? AND COALESCE(product_type,'joya') != 'joyero'",
        (f"{current_month}%",)
    ).fetchone()["s"]
    ventas_ano = conn.execute(
        "SELECT COUNT(*) as c FROM orders WHERE fecha_pedido LIKE ? AND COALESCE(product_type,'joya') != 'joyero'",
        (f"{current_year}%",)
    ).fetchone()["c"]
    facturacion_ano = conn.execute(
        "SELECT COALESCE(SUM(pvp / 1.21),0) as s FROM orders WHERE fecha_pedido LIKE ? AND COALESCE(product_type,'joya') != 'joyero'",
        (f"{current_year}%",)
    ).fetchone()["s"]
    mes_nombre = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"][now.month - 1]

    conn.close()
    return {
        "total": total,
        "pending": pending,
        "in_workshop": in_workshop,
        "revenue": revenue,
        "avg_ticket": avg_ticket,
        "joyas_count": joyas_count,
        "joyeros_count": joyeros_count,
        "cadenas_count": cadenas_count,
        "unique_tickets": unique_tickets,
        "avg_ticket_joyas": avg_ticket_joyas,
        "avg_ticket_joyas_iva": avg_ticket_joyas_iva,
        "ventas_mes": ventas_mes,
        "facturacion_mes": facturacion_mes,
        "ventas_ano": ventas_ano,
        "facturacion_ano": facturacion_ano,
        "mes_nombre": mes_nombre,
    }


# ── Supplier summaries ──
def get_supplier_summary(supplier: str):
    """supplier = 'lola' or 'barto'"""
    conn = get_db()
    est_col = f"{supplier}_estimado"
    real_col = f"{supplier}_real"
    date_col = f"fecha_cobro_{supplier}"

    total_est = conn.execute(f"SELECT COALESCE(SUM({est_col}),0) as s FROM orders").fetchone()["s"]
    total_real = conn.execute(f"SELECT COALESCE(SUM({real_col}),0) as s FROM orders WHERE {real_col} IS NOT NULL").fetchone()["s"]
    pending_payment = conn.execute(
        f"SELECT * FROM orders WHERE {real_col} IS NOT NULL AND ({date_col} IS NULL OR {date_col}='') ORDER BY fecha_pedido DESC"
    ).fetchall()
    paid = conn.execute(
        f"SELECT * FROM orders WHERE {date_col} IS NOT NULL AND {date_col}!='' ORDER BY {date_col} DESC"
    ).fetchall()

    conn.close()
    return {
        "total_estimado": total_est,
        "total_real": total_real,
        "pending_payment": rows_to_list(pending_payment),
        "paid": rows_to_list(paid),
    }


# ── Supplier portal ──
def get_supplier_orders(supplier: str):
    """Get orders relevant to a supplier.
    barto: all non-delivered orders (he's responsible for the whole workshop)
    lola: all non-delivered orders that have piedras (lola_estimado > 0 or piedras_desc not empty)
    Returns dict with 'pending' and 'completed' lists.
    """
    conn = get_db()
    if supplier == "barto":
        pending = conn.execute(
            "SELECT * FROM orders WHERE status != 'entregado' AND COALESCE(product_type,'joya') != 'joyero' ORDER BY fecha_pedido DESC"
        ).fetchall()
        completed = conn.execute(
            "SELECT * FROM orders WHERE joya_terminada = '1' ORDER BY joya_terminada_at DESC LIMIT 50"
        ).fetchall()
    elif supplier == "lola":
        pending = conn.execute(
            """SELECT * FROM orders
               WHERE status != 'entregado'
                 AND (COALESCE(lola_estimado, 0) > 0 OR (piedras_desc IS NOT NULL AND piedras_desc != ''))
                 AND piedras_entregadas != '1'
               ORDER BY fecha_pedido DESC"""
        ).fetchall()
        completed = conn.execute(
            """SELECT * FROM orders
               WHERE piedras_entregadas = '1'
                 AND (COALESCE(lola_estimado, 0) > 0 OR (piedras_desc IS NOT NULL AND piedras_desc != ''))
               ORDER BY piedras_entregadas_at DESC LIMIT 50"""
        ).fetchall()
    else:
        conn.close()
        return {"pending": [], "completed": []}
    conn.close()
    return {"pending": rows_to_list(pending), "completed": rows_to_list(completed)}


def mark_piedras_entregadas(order_id: int):
    """Lola marks stones delivered to Barto. Sets piedras_entregadas='1' and timestamp."""
    conn = get_db()
    now = datetime.datetime.now().isoformat()
    conn.execute(
        "UPDATE orders SET piedras_entregadas='1', piedras_entregadas_at=?, updated_at=? WHERE id=?",
        (now, now, order_id)
    )
    conn.commit()
    conn.close()
    log_activity(order_id, "Piedras entregadas a Barto", f"Lola marcó entrega el {now}")


def mark_joya_terminada(order_id: int):
    """Barto marks piece finished. Sets joya_terminada='1', timestamp, captures gold price, changes status to 'entregado'."""
    from gold_price import get_current_gold_price
    conn = get_db()
    now = datetime.datetime.now().isoformat()

    # Get order to calculate gold cost
    row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    if not row:
        conn.close()
        return

    order = dict(row)
    real_gold = get_current_gold_price()
    peso_est = float(order.get("peso_estimado", 0) or 0)
    oro_real = peso_est * real_gold

    conn.execute(
        """UPDATE orders SET
            joya_terminada='1', joya_terminada_at=?,
            precio_oro_real=?, oro_total_real=?,
            status='entregado', updated_at=?
           WHERE id=?""",
        (now, round(real_gold, 2), round(oro_real, 2), now, order_id)
    )
    conn.commit()
    conn.close()
    log_activity(order_id, "Joya terminada por Barto",
                 f"Oro 24K: {real_gold:.2f} EUR/gr x {peso_est:.1f}gr = {oro_real:.2f} EUR")
    log_activity(order_id, "Estado cambiado a entregado")


# ── Bank entries CRUD ──
def insert_bank_entry(data: dict) -> int:
    conn = get_db()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    cur = conn.execute(f"INSERT INTO bank_entries ({cols}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    entry_id = cur.lastrowid
    conn.close()
    return entry_id


def get_bank_entries(month=None, from_month=None, to_month=None, categoria=None, unmatched_only=False):
    conn = get_db()
    sql = "SELECT * FROM bank_entries WHERE 1=1"
    params = []
    if from_month and to_month:
        sql += " AND fecha >= ? AND fecha < ?"
        params.append(f"{from_month}-01")
        # Add 1 month to to_month for < comparison
        y, m = int(to_month[:4]), int(to_month[5:7])
        m += 1
        if m > 12:
            m = 1
            y += 1
        params.append(f"{y}-{m:02d}-01")
    elif month:
        sql += " AND fecha LIKE ?"
        params.append(f"{month}%")
    if categoria:
        sql += " AND categoria=?"
        params.append(categoria)
    if unmatched_only:
        sql += " AND matched_order_id IS NULL"
    sql += " ORDER BY fecha DESC, id DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows_to_list(rows)


def update_bank_entry(entry_id: int, data: dict):
    conn = get_db()
    sets = ", ".join([f"{k}=?" for k in data])
    conn.execute(f"UPDATE bank_entries SET {sets} WHERE id=?", list(data.values()) + [entry_id])
    conn.commit()
    conn.close()


# ── Cash sales CRUD ──
def insert_cash_sale(data: dict) -> int:
    conn = get_db()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    cur = conn.execute(f"INSERT INTO cash_sales ({cols}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    sale_id = cur.lastrowid
    conn.close()
    return sale_id


def get_cash_sales(month=None, from_month=None, to_month=None):
    conn = get_db()
    sql = "SELECT * FROM cash_sales WHERE 1=1"
    params = []
    if from_month and to_month:
        sql += " AND fecha >= ? AND fecha < ?"
        params.append(f"{from_month}-01")
        y, m = int(to_month[:4]), int(to_month[5:7])
        m += 1
        if m > 12:
            m = 1
            y += 1
        params.append(f"{y}-{m:02d}-01")
    elif month:
        sql += " AND fecha LIKE ?"
        params.append(f"{month}%")
    sql += " ORDER BY fecha DESC, id DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows_to_list(rows)


def delete_cash_sale(sale_id: int):
    conn = get_db()
    conn.execute("DELETE FROM cash_sales WHERE id=?", (sale_id,))
    conn.commit()
    conn.close()


# ── Accounting stats ──
def get_accounting_stats(month=None, from_month=None, to_month=None):
    import datetime as _dt
    if not from_month:
        if month:
            from_month = month
            to_month = month
        else:
            from_month = _dt.date.today().strftime("%Y-%m")
            to_month = from_month
    if not to_month:
        to_month = from_month

    # Build date range: from_month-01 to (to_month + 1 month)-01
    date_start = f"{from_month}-01"
    y, m = int(to_month[:4]), int(to_month[5:7])
    m += 1
    if m > 12:
        m = 1
        y += 1
    date_end = f"{y}-{m:02d}-01"

    conn = get_db()

    def _sum_bank(where_clause):
        sql = f"SELECT COALESCE(SUM(importe), 0) as s FROM bank_entries WHERE fecha >= ? AND fecha < ? AND ({where_clause})"
        return conn.execute(sql, [date_start, date_end]).fetchone()["s"]

    def _sum_bank_abs(where_clause):
        sql = f"SELECT COALESCE(SUM(ABS(importe)), 0) as s FROM bank_entries WHERE fecha >= ? AND fecha < ? AND ({where_clause})"
        return conn.execute(sql, [date_start, date_end]).fetchone()["s"]

    # Ingresos reales en banco (neto, lo que entra en la cuenta)
    ingresos_shopify = _sum_bank("categoria='shopify_payout' AND importe > 0")
    ingresos_transferencia = _sum_bank("categoria='transferencia_cliente' AND importe > 0")
    ingresos_paypal = _sum_bank("categoria='paypal' AND importe > 0")

    cash_row = conn.execute(
        "SELECT COALESCE(SUM(importe), 0) as s FROM cash_sales WHERE fecha >= ? AND fecha < ?",
        (date_start, date_end)
    ).fetchone()
    ingresos_efectivo = cash_row["s"]

    total_ingresos = ingresos_shopify + ingresos_transferencia + ingresos_paypal + ingresos_efectivo

    gastos_taller = _sum_bank_abs("categoria IN ('gasto_taller', 'gasto_piedras') AND importe < 0")
    gastos_fijos = _sum_bank_abs("categoria IN ('alquiler', 'saas', 'packaging', 'otro_gasto', 'telefonia', 'envios', 'asesoria', 'formacion', 'fotografia', 'comision_transferencia') AND importe < 0")
    gastos_nominas = _sum_bank_abs("categoria IN ('nomina', 'seguridad_social') AND importe < 0")
    gastos_publicidad = _sum_bank_abs("categoria='publicidad' AND importe < 0")
    gastos_impuestos = _sum_bank_abs("categoria='impuestos' AND importe < 0")
    gastos_comisiones = _sum_bank_abs("categoria IN ('comision_shopify', 'comision_paypal') AND importe < 0")

    gastos_traspasos = _sum_bank_abs("categoria IN ('traspaso_interno', 'transferencia_internacional') AND importe < 0")

    total_gastos = gastos_taller + gastos_fijos + gastos_nominas + gastos_publicidad + gastos_impuestos + gastos_comisiones
    resultado = total_ingresos - total_gastos

    # Shopify orders total (from orders table, not bank)
    ventas_shopify = conn.execute(
        "SELECT COALESCE(SUM(pvp), 0) as s FROM orders WHERE fecha_pedido >= ? AND fecha_pedido < ?",
        (date_start, date_end)
    ).fetchone()["s"]

    # Total ingresos en banco (todos los positivos, excluyendo shopify payouts que ya se cuentan)
    ingresos_transferencias_banco = _sum_bank("categoria='transferencia_cliente' AND importe > 0")
    ingresos_paypal_banco = _sum_bank("categoria='paypal' AND importe > 0")

    # Total cobrado = shopify payouts + transferencias + paypal (lo que realmente entra en banco por ventas)
    total_cobrado_banco = ingresos_shopify + ingresos_transferencias_banco + ingresos_paypal_banco

    # Comisiones Shopify = diferencia entre PVP de pedidos SP y lo que llega al banco
    # Desviacion = ventas Shopify - (cobrado en banco + comisiones)
    # Si es positiva = hay dinero pendiente de cobrar
    # Si es negativa = hay mas cobrado que vendido (transferencias de otros periodos)
    desviacion = ventas_shopify - total_cobrado_banco

    conn.close()
    return {
        "from_month": from_month,
        "to_month": to_month,
        "ingresos_shopify": ingresos_shopify,
        "ingresos_transferencia": ingresos_transferencia,
        "ingresos_paypal": ingresos_paypal,
        "ingresos_efectivo": ingresos_efectivo,
        "total_ingresos": total_ingresos,
        "gastos_taller": gastos_taller,
        "gastos_fijos": gastos_fijos,
        "gastos_nominas": gastos_nominas,
        "gastos_publicidad": gastos_publicidad,
        "gastos_impuestos": gastos_impuestos,
        "gastos_comisiones": gastos_comisiones,
        "gastos_traspasos": gastos_traspasos,
        "total_gastos": total_gastos,
        "resultado": resultado,
        "ventas_shopify": ventas_shopify,
        "total_cobrado_banco": total_cobrado_banco,
        "desviacion": desviacion,
    }


# ── Leads CRUD ──
def insert_lead(data: dict) -> int:
    conn = get_db()
    now = datetime.datetime.now().isoformat()
    data["created_at"] = now
    data["updated_at"] = now
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    cur = conn.execute(f"INSERT INTO leads ({cols}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    lead_id = cur.lastrowid
    conn.close()
    return lead_id


def get_lead(lead_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


def get_all_leads(estado=None, via=None, search=None):
    conn = get_db()
    sql = "SELECT * FROM leads WHERE 1=1"
    params = []
    if estado and estado != "todos":
        sql += " AND estado=?"
        params.append(estado)
    if via and via != "todos":
        sql += " AND via_contacto=?"
        params.append(via)
    if search:
        sql += " AND (nombre LIKE ? OR email LIKE ? OR telefono LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    sql += " ORDER BY created_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows_to_list(rows)


def update_lead(lead_id: int, data: dict):
    conn = get_db()
    data["updated_at"] = datetime.datetime.now().isoformat()
    sets = ", ".join([f"{k}=?" for k in data])
    conn.execute(f"UPDATE leads SET {sets} WHERE id=?", list(data.values()) + [lead_id])
    conn.commit()
    conn.close()


def get_lead_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM leads").fetchone()["c"]
    by_estado = {}
    for estado in ['nuevo', 'contactado', 'cita_reservada', 'compra', 'no_compra', 'cancelado']:
        by_estado[estado] = conn.execute("SELECT COUNT(*) as c FROM leads WHERE estado=?", (estado,)).fetchone()["c"]
    by_via = {}
    for via in ['instagram', 'whatsapp', 'web', 'calendly', 'otro']:
        by_via[via] = conn.execute("SELECT COUNT(*) as c FROM leads WHERE via_contacto=?", (via,)).fetchone()["c"]

    compras = by_estado.get('compra', 0)
    conversion_rate = (compras / total * 100) if total > 0 else 0

    conn.close()
    return {
        "total": total,
        "by_estado": by_estado,
        "by_via": by_via,
        "conversion_rate": round(conversion_rate, 1),
    }


def normalize_phone(phone: str) -> str:
    """Normalize phone for matching: strip spaces, dashes, country prefix."""
    if not phone:
        return ""
    import re
    phone = re.sub(r'[\s\-\(\)\.]', '', phone)
    # Remove common Spanish prefixes
    if phone.startswith('+34'):
        phone = phone[3:]
    elif phone.startswith('0034'):
        phone = phone[4:]
    elif phone.startswith('34') and len(phone) > 9:
        phone = phone[2:]
    return phone


def match_lead_to_order(customer_email: str, customer_phone: str, customer_name: str) -> int | None:
    """Find a lead that matches the given customer data. Returns lead_id or None."""
    conn = get_db()
    lead = None

    # 1. Exact email match (most reliable)
    if customer_email:
        lead = conn.execute(
            "SELECT id FROM leads WHERE LOWER(email)=LOWER(?) AND estado NOT IN ('compra','cancelado') ORDER BY created_at DESC LIMIT 1",
            (customer_email,)
        ).fetchone()

    # 2. Phone match (normalized)
    if not lead and customer_phone:
        norm_phone = normalize_phone(customer_phone)
        if norm_phone:
            all_leads = conn.execute(
                "SELECT id, telefono FROM leads WHERE estado NOT IN ('compra','cancelado') AND telefono IS NOT NULL AND telefono != ''"
            ).fetchall()
            for l in all_leads:
                if normalize_phone(l["telefono"]) == norm_phone:
                    lead = l
                    break

    # 3. Name match (fuzzy - only if exact-ish)
    if not lead and customer_name:
        name_parts = customer_name.lower().strip().split()
        if len(name_parts) >= 2:
            lead = conn.execute(
                "SELECT id FROM leads WHERE LOWER(nombre) LIKE ? AND LOWER(nombre) LIKE ? AND estado NOT IN ('compra','cancelado') ORDER BY created_at DESC LIMIT 1",
                (f"%{name_parts[0]}%", f"%{name_parts[-1]}%")
            ).fetchone()

    conn.close()
    return lead["id"] if lead else None


def convert_lead(lead_id: int, shopify_order_id: str):
    """Mark a lead as converted to purchase."""
    now = datetime.datetime.now().isoformat()
    update_lead(lead_id, {
        "estado": "compra",
        "shopify_order_id": shopify_order_id,
        "converted_at": now,
    })


# ── Production phases ──
PRODUCTION_PHASES = ['pendiente', 'prototipado', 'fundido', 'repaso', 'engaste', 'repaso_final', 'terminado']


def advance_production_phase(order_id: int, new_phase: str):
    """Update production phase for an order."""
    if new_phase not in PRODUCTION_PHASES:
        raise ValueError(f"Fase no válida: {new_phase}")
    conn = get_db()
    now = datetime.datetime.now().isoformat()
    conn.execute(
        "UPDATE orders SET fase_produccion=?, fase_updated_at=?, updated_at=? WHERE id=?",
        (new_phase, now, now, order_id)
    )
    conn.commit()
    conn.close()
    log_activity(order_id, f"Fase de producción: {new_phase}", f"Actualizado el {now}")


if __name__ == "__main__":
    init_db()
    print("Database initialized at", DB_PATH)
