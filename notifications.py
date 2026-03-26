"""
Supplier notification system: Email (SMTP) + WhatsApp (click-to-send links).
Lola = MAS GEMAS (piedras de color)
Barto = NOVAO MB18 SL (taller + diamantes)
"""
import os, smtplib, urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

# Supplier info
SUPPLIERS = {
    "lola": {
        "name": "Lola",
        "company": "MAS GEMAS",
        "email": "info@masgemasyjoyas.es",
        "phone": "34666588527",
        "role": "piedras de color",
    },
    "barto": {
        "name": "Barto",
        "company": "NOVAO MB18 SL",
        "email": "novaomb18@gmail.com",
        "phone": "34659319904",
        "role": "taller y diamantes",
    },
}

# SMTP config
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_EMAIL", os.getenv("SMTP_USER", ""))
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")


def email_template_lola(order: dict) -> tuple:
    """Returns (subject, body) for Lola notification."""
    piece = order.get("product_name", "Pieza")
    size = order.get("ring_size", "")
    size_txt = f"\n- Talla: {size}" if size else ""
    est = order.get("lola_estimado", 0)
    variant = order.get("variant", "")
    variant_txt = f"\n- Variante: {variant}" if variant else ""

    subject = f"Nuevo pedido Antiqua - {piece}"
    body = f"""Hola Lola,

Tenemos un nuevo pedido:
- Pieza: {piece}{size_txt}{variant_txt}
- Presupuesto estimado piedras: {est:.0f} EUR

¿Puedes confirmar disponibilidad y plazo?

Gracias,
MIMA - Asistente de Antiqua"""
    return subject, body


def email_template_barto(order: dict) -> tuple:
    """Returns (subject, body) for Barto notification."""
    piece = order.get("product_name", "Pieza")
    size = order.get("ring_size", "")
    size_txt = f"\n- Talla: {size}" if size else ""
    est_taller = order.get("barto_estimado", 0)
    peso = order.get("peso_estimado", 0)

    subject = f"Nuevo pedido Antiqua - {piece}"
    body = f"""Hola Barto,

Nuevo pedido para el taller:
- Pieza: {piece}{size_txt}
- Peso estimado: {peso:.1f} gr
- Presupuesto taller + diamantes: {est_taller:.0f} EUR

¿Plazo estimado de entrega?

Gracias,
MIMA - Asistente de Antiqua"""
    return subject, body


def whatsapp_template_lola(order: dict) -> str:
    """Short WhatsApp message for Lola."""
    piece = order.get("product_name", "Pieza")
    size = order.get("ring_size", "")
    size_txt = f", talla {size}" if size else ""
    est = order.get("lola_estimado", 0)
    return f"Hola Lola! Nuevo pedido Antiqua: {piece}{size_txt}. Estimado piedras: {est:.0f}EUR. ¿Disponibilidad y plazo?"


def whatsapp_template_barto(order: dict) -> str:
    """Short WhatsApp message for Barto."""
    piece = order.get("product_name", "Pieza")
    size = order.get("ring_size", "")
    size_txt = f", talla {size}" if size else ""
    peso = order.get("peso_estimado", 0)
    est = order.get("barto_estimado", 0)
    return f"Hola Barto! Nuevo pedido Antiqua: {piece}{size_txt}. Peso ~{peso:.1f}gr, estimado taller+diamantes: {est:.0f}EUR. ¿Plazo?"


def generate_whatsapp_link(supplier: str, order: dict) -> str:
    """Generate wa.me link with pre-filled message."""
    info = SUPPLIERS.get(supplier, {})
    phone = info.get("phone", "")

    if supplier == "lola":
        msg = whatsapp_template_lola(order)
    else:
        msg = whatsapp_template_barto(order)

    encoded = urllib.parse.quote(msg)
    return f"https://wa.me/{phone}?text={encoded}"


def send_email(to: str, subject: str, body: str) -> bool:
    """Send email via SMTP. Returns True if successful."""
    if not SMTP_USER or not SMTP_PASSWORD:
        print(f"SMTP not configured. Would send to {to}: {subject}")
        return False

    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to, msg.as_string())
        print(f"Email sent to {to}: {subject}")
        return True
    except Exception as e:
        print(f"Email failed to {to}: {e}")
        return False


def notify_supplier(supplier: str, order: dict) -> dict:
    """
    Send email + generate WhatsApp link for a supplier.
    Returns {"email_sent": bool, "whatsapp_link": str}
    """
    info = SUPPLIERS.get(supplier, {})
    if not info:
        return {"email_sent": False, "whatsapp_link": ""}

    # Email
    if supplier == "lola":
        subject, body = email_template_lola(order)
    else:
        subject, body = email_template_barto(order)

    email_sent = send_email(info["email"], subject, body)

    # WhatsApp
    wa_link = generate_whatsapp_link(supplier, order)

    return {
        "email_sent": email_sent,
        "whatsapp_link": wa_link,
        "email_to": info["email"],
        "email_subject": subject,
        "email_body": body,
    }


def get_notification_preview(order: dict) -> dict:
    """Get preview of both supplier notifications without sending."""
    lola_subj, lola_body = email_template_lola(order)
    barto_subj, barto_body = email_template_barto(order)

    return {
        "lola": {
            "email_subject": lola_subj,
            "email_body": lola_body,
            "whatsapp_link": generate_whatsapp_link("lola", order),
            "whatsapp_msg": whatsapp_template_lola(order),
            "to": SUPPLIERS["lola"]["email"],
        },
        "barto": {
            "email_subject": barto_subj,
            "email_body": barto_body,
            "whatsapp_link": generate_whatsapp_link("barto", order),
            "whatsapp_msg": whatsapp_template_barto(order),
            "to": SUPPLIERS["barto"]["email"],
        },
    }
