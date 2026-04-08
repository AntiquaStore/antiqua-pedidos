"""
Bank reconciliation module for Antiqua accounting.
Parses CaixaBank CSV exports and auto-categorizes entries.
"""
import csv
import io
import re


def parse_bank_csv(csv_content_or_path):
    """Parse CaixaBank CSV (semicolon delimited).
    Format: Concepto;Fecha;Importe;Saldo;;
    - Skip rows where col 5 contains '2025' (header/summary rows)
    - Parse date dd/mm/yyyy -> yyyy-mm-dd
    - Parse amount: strip EUR, dots(thousands), comma->dot
    - tipo: 'ingreso' if positive, 'gasto' if negative
    Returns list of dicts.
    """
    # Determine if it's a file path or content string
    if isinstance(csv_content_or_path, (bytes, bytearray)):
        csv_content_or_path = csv_content_or_path.decode("utf-8-sig")

    if "\n" not in csv_content_or_path and "\r" not in csv_content_or_path:
        # Looks like a file path
        with open(csv_content_or_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
    else:
        content = csv_content_or_path

    entries = []
    reader = csv.reader(io.StringIO(content), delimiter=";")

    for row in reader:
        if len(row) < 4:
            continue

        # Skip header/summary rows: if column index 4 (5th col) contains '2025'
        if len(row) > 4 and "2025" in str(row[4]):
            continue

        concepto = row[0].strip()
        fecha_raw = row[1].strip()
        importe_raw = row[2].strip()
        saldo_raw = row[3].strip()

        # Skip rows without a valid date
        if not re.match(r"\d{2}/\d{2}/\d{4}", fecha_raw):
            continue

        # Parse date dd/mm/yyyy -> yyyy-mm-dd
        try:
            parts = fecha_raw.split("/")
            fecha = f"{parts[2]}-{parts[1]}-{parts[0]}"
        except (IndexError, ValueError):
            continue

        # Parse amount: strip EUR, dots(thousands), comma->dot
        importe = _parse_amount(importe_raw)
        saldo = _parse_amount(saldo_raw)

        if importe is None:
            continue

        tipo = "ingreso" if importe >= 0 else "gasto"

        entries.append({
            "concepto": concepto,
            "fecha": fecha,
            "importe": round(importe, 2),
            "saldo": round(saldo, 2) if saldo is not None else None,
            "tipo": tipo,
        })

    return entries


def _parse_amount(raw: str):
    """Parse amount string like '+1.234,56EUR' or '-1.234,56EUR' to float."""
    if not raw:
        return None
    cleaned = raw.strip()
    cleaned = cleaned.replace("EUR", "").replace("€", "").strip()
    # Remove thousand separators (dots) and convert decimal comma to dot
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def categorize_entry(concepto, importe):
    """Auto-categorize by concepto patterns. Returns categoria string."""
    c = concepto.upper() if concepto else ""
    is_positive = (importe or 0) >= 0

    if is_positive:
        if "TRANSFER. EN DIV" in c:
            return "shopify_payout"
        if "TRANSF. A SU FAVOR" in c or "TRANSFER INMEDIATA" in c:
            return "transferencia_cliente"
        if "TRASPASO" in c:
            return "paypal"
        if "CLICK RENT" in c:
            return "otro_ingreso"
        return "otro_ingreso"
    else:
        # Negative amounts (gastos)
        if "NOMINA" in c or "ITZIAR" in c:
            return "nomina"
        if "TGSS" in c:
            return "seguridad_social"
        if "IMPUESTOS AEAT" in c:
            return "impuestos"
        if "FACEBK" in c:
            return "publicidad"
        if "KLAVIYO" in c:
            return "saas"
        if "HOLDED" in c:
            return "saas"
        if "GOOGLE WORKSPACE" in c:
            return "saas"
        if "APPLE.COM" in c:
            return "saas"
        if "SHOPIFY*" in c or "SHOPIFY" in c:
            return "comision_shopify"
        # Taller: antiqua + nov/n (Novao payments often abbreviated)
        if "NOVAO" in c:
            return "gasto_taller"
        if "ANTIQUA" in c and ("NOV" in c or c.rstrip().endswith("N") or " N" in c):
            return "gasto_taller"
        # Piedras: antiqua + lo/ma (Lola / Mas Gemas payments)
        if "MAS GEMAS" in c:
            return "gasto_piedras"
        if "ANTIQUA" in c and ("LO" in c or " MA" in c or c.rstrip().endswith("MA")):
            return "gasto_piedras"
        if "NEGUERUEL" in c:
            return "gasto_piedras"
        if "FINETWORK" in c or "MASMOVIL" in c:
            return "telefonia"
        if "PACKLINK" in c or "UPS" in c:
            return "envios"
        if "PACKHELP" in c:
            return "packaging"
        if "ARFE" in c or "YOU" in c:
            return "asesoria"
        if "PAGO TRASPASOS" in c:
            return "comision_paypal"
        if "REBUNDLE" in c:
            return "formacion"
        # Traspasos internos entre cuentas
        if "TRANSF. INSTANTANEA" in c or "TRANSF. A SU FAVOR" in c:
            return "traspaso_interno"
        if "TRF.INTERNACIONAL" in c:
            return "transferencia_internacional"
        if "SERV. EM. TRANSF" in c:
            return "comision_transferencia"
        # Martin Valentin, Jose Cruz, Jose Alvarez = pagos a proveedores varios
        if "JOSE MANUEL ALVAR" in c or "JOSE M ALVAREZ" in c:
            return "gasto_taller"  # Joyero Jose Manuel
        if "JOSE LUIZ CRUZ" in c or "JOSE LUI" in c:
            return "gasto_taller"
        if "FOKKELMAN" in c:
            return "gasto_piedras"  # Gemologist
        if "FOTOCASION" in c or "REVELADO" in c or "JHULIEN" in c:
            return "fotografia"
        if "DIRECTORIO" in c or "CERTIFICADO" in c:
            return "otro_gasto"
        return "otro_gasto"


def match_with_orders(bank_entries, orders):
    """Try to match bank ingresos with Shopify orders by amount (tolerance < 1 EUR).
    Returns bank_entries with matched_order_id set where possible.
    """
    # Build list of unmatched orders with their PVP
    available_orders = []
    for o in orders:
        pvp = float(o.get("pvp", 0) or 0)
        if pvp > 0:
            available_orders.append({"id": o["id"], "pvp": pvp, "matched": False})

    for entry in bank_entries:
        if entry.get("tipo") != "ingreso":
            continue
        importe = abs(float(entry.get("importe", 0)))
        if importe <= 0:
            continue

        # Try to find a matching order
        best_match = None
        best_diff = 999999
        for order in available_orders:
            if order["matched"]:
                continue
            diff = abs(importe - order["pvp"])
            if diff < 1.0 and diff < best_diff:
                best_match = order
                best_diff = diff

        if best_match:
            entry["matched_order_id"] = best_match["id"]
            best_match["matched"] = True

    return bank_entries
