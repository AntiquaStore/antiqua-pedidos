"""
Supplier notification system: Email (SMTP) + WhatsApp (click-to-send links).
Lola = MAS GEMAS (piedras de color)
Barto = NOVAO MB18 SL (taller + diamantes)
"""
import os, smtplib, urllib.parse
from datetime import date, timedelta
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
    piedras_desc = order.get("piedras_desc", "")

    subject = f"Pedido Antiqua - {piece}"
    if piedras_desc:
        body = f"Hola Lola, necesitamos {piedras_desc} para la sortija {piece} porfa. Se las dejas a Barto?\n\nGracias,\nMIMA - Asistente de Antiqua"
    else:
        body = f"Hola Lola, necesitamos las piedras de color para la sortija {piece} porfa. Se las dejas a Barto?\n\nGracias,\nMIMA - Asistente de Antiqua"
    return subject, body


def add_business_days(start: date, days: int) -> date:
    """Add N business days (Mon-Fri) to a date."""
    current = start
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon=0 .. Fri=4
            added += 1
    return current


def fecha_limite_entrega() -> str:
    """Calculate delivery deadline: 18 business days from today."""
    deadline = add_business_days(date.today(), 18)
    # Format: "15 de abril"
    meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
             "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    return f"{deadline.day} de {meses[deadline.month - 1]}"


def email_template_barto(order: dict) -> tuple:
    """Returns (subject, body) for Barto notification."""
    piece = order.get("product_name", "Pieza")
    size = order.get("ring_size", "")
    size_txt = f" talla {size}" if size else ""
    fecha = fecha_limite_entrega()

    subject = f"Nuevo pedido - Sortija {piece}"
    body = f"Nuevo pedido - Sortija {piece}{size_txt} - fecha de entrega limite al cliente {fecha}\n\nMIMA - Asistente de Antiqua"
    return subject, body


def whatsapp_template_lola(order: dict) -> str:
    """Short WhatsApp message for Lola."""
    piece = order.get("product_name", "Pieza")
    piedras_desc = order.get("piedras_desc", "")
    if piedras_desc:
        return f"Hola Lola, necesitamos {piedras_desc} para la sortija {piece} porfa. Se las dejas a Barto?"
    else:
        return f"Hola Lola, necesitamos las piedras de color para la sortija {piece} porfa. Se las dejas a Barto?"


def whatsapp_template_barto(order: dict) -> str:
    """Short WhatsApp message for Barto."""
    piece = order.get("product_name", "Pieza")
    size = order.get("ring_size", "")
    size_txt = f" talla {size}" if size else ""
    fecha = fecha_limite_entrega()
    return f"Nuevo pedido - Sortija {piece}{size_txt} - fecha de entrega limite al cliente {fecha}"


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
