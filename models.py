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
    ]:
        try:
            conn.execute(f"ALTER TABLE orders ADD COLUMN {col} {coltype}")
        except Exception:
            pass  # Column already exists

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
    updates = ", ".join([f"{k}=excluded.{k}" for k in data if k not in ("shopify_order_id", "product_name", "created_at")])

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
    sql += " ORDER BY fecha_pedido DESC"
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
    notified = conn.execute("SELECT COUNT(*) as c FROM orders WHERE status='notificado'").fetchone()["c"]
    in_workshop = conn.execute("SELECT COUNT(*) as c FROM orders WHERE status='en_taller'").fetchone()["c"]
    revenue = conn.execute("SELECT COALESCE(SUM(pvp / 1.21),0) as s FROM orders").fetchone()["s"]

    # Joyas vs Joyeros vs Cadenas counts (partial payments grouped = 1 joya)
    joyas_grouped = conn.execute(
        "SELECT COUNT(DISTINCT payment_group) as c FROM orders WHERE COALESCE(product_type,'joya') = 'joya' AND payment_group IS NOT NULL AND payment_group != ''"
    ).fetchone()["c"]
    joyas_ungrouped = conn.execute(
        "SELECT COUNT(*) as c FROM orders WHERE COALESCE(product_type,'joya') = 'joya' AND (payment_group IS NULL OR payment_group = '')"
    ).fetchone()["c"]
    joyas_count = joyas_grouped + joyas_ungrouped
    joyeros_count = conn.execute(
        "SELECT COUNT(*) as c FROM orders WHERE product_type = 'joyero'"
    ).fetchone()["c"]
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
        "notified": notified,
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
    barto: all notified/en_taller orders
    lola: all notified/en_taller orders that have piedras (lola_estimado > 0 or piedras_desc not empty)
    Returns dict with 'pending' and 'completed' lists.
    """
    conn = get_db()
    if supplier == "barto":
        pending = conn.execute(
            "SELECT * FROM orders WHERE status IN ('notificado', 'en_taller') ORDER BY fecha_pedido DESC"
        ).fetchall()
        completed = conn.execute(
            "SELECT * FROM orders WHERE joya_terminada = '1' ORDER BY joya_terminada_at DESC LIMIT 50"
        ).fetchall()
    elif supplier == "lola":
        pending = conn.execute(
            """SELECT * FROM orders
               WHERE status IN ('notificado', 'en_taller')
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


if __name__ == "__main__":
    init_db()
    print("Database initialized at", DB_PATH)
