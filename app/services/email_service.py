import os
import smtplib
import mimetypes
import html
from pathlib import Path
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_FROM = os.getenv("MAIL_FROM")

BASE_DIR = Path(__file__).resolve().parents[2]
LOGO_PATH = BASE_DIR / "app" / "api" / "project_media" / "logo.png"


def _normalize_recipients(to_email: str | list[str]) -> list[str]:
    if isinstance(to_email, str):
        return [to_email.strip()] if to_email.strip() else []
    return [email.strip() for email in to_email if email and email.strip()]


def _escape_and_format_body(body: str) -> str:
    escaped = html.escape(body.strip())
    return escaped.replace("\n", "<br>")


def _build_plain_template(
    body: str,
    title: str | None = None,
    action_link: str | None = None,
) -> str:
    title_text = title or "Trade-AI Notification"

    link_block = f"\nLink: {action_link}\n" if action_link else "\n"

    return f"""
TRADE-AI APP
{title_text}
{"=" * len(title_text)}

{body}
{link_block}
--------------------------------------------------
TRADE AI APP @2026
""".strip()


def _build_html_template(
    body: str,
    title: str | None = None,
    include_logo: bool = True,
    action_link: str | None = None,
) -> str:
    title_text = html.escape(title or "Trade-AI Notification")
    formatted_body = _escape_and_format_body(body)

    logo_html = (
        """
        <img src="cid:tradeai_logo"
             alt="Trade-AI Logo"
             style="height:38px; width:auto; display:block;" />
        """
        if include_logo
        else ""
    )

    action_section = ""
    if action_link and action_link.strip():
        safe_link = html.escape(action_link.strip(), quote=True)
        action_section = f"""
        <div style="margin-top:20px; text-align:center;">
            <a href="{safe_link}"
               style="
                   display:inline-block;
                   background:#2563eb;
                   color:#ffffff;
                   text-decoration:none;
                   font-size:14px;
                   font-weight:700;
                   padding:12px 18px;
                   border-radius:10px;
               ">
                Open Related Link
            </a>
        </div>

        <div style="
            margin-top:12px;
            font-size:12px;
            color:#6b7280;
            text-align:center;
            word-break:break-all;
        ">
            {safe_link}
        </div>
        """

    return f"""
    <html>
        <body style="margin:0; padding:0; background-color:#eef2f7; font-family:Arial, Helvetica, sans-serif; color:#1f2937;">
            <div style="max-width:760px; margin:28px auto; padding:0 12px;">

                <div style="
                    background:#ffffff;
                    border:1px solid #dbe3ec;
                    border-radius:18px;
                    overflow:hidden;
                    box-shadow:0 8px 24px rgba(17,24,39,0.06);
                ">

                    <!-- HEADER -->
                    <div style="
                        background:linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
                        padding:22px 24px;
                    ">
                        <div style="display:flex; align-items:center;">
                            <div style="margin-right:14px;">
                                {logo_html}
                            </div>

                            <div style="color:#ffffff;">
                                <div style="font-size:22px; font-weight:700; line-height:1.2;">
                                    Trade-AI App
                                </div>
                                <div style="font-size:12px; color:#cbd5e1; margin-top:4px; letter-spacing:0.2px;">
                                    Automated Notification System
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- TITLE -->
                    <div style="padding:24px 24px 0 24px;">
                        <div style="
                            font-size:22px;
                            font-weight:700;
                            color:#111827;
                            line-height:1.3;
                            margin-bottom:12px;
                        ">
                            {title_text}
                        </div>

                        <div style="
                            font-size:13px;
                            color:#6b7280;
                            background:#f8fafc;
                            border:1px solid #e5e7eb;
                            border-radius:10px;
                            padding:10px 12px;
                        ">
                            This notification was generated automatically by the Trade-AI monitoring system.
                        </div>
                    </div>

                    <!-- BODY CARD -->
                    <div style="padding:18px 24px 8px 24px;">
                        <div style="
                            background:#f9fafb;
                            border:1px solid #e5e7eb;
                            border-radius:14px;
                            padding:20px;
                        ">
                            <div style="
                                font-size:14px;
                                line-height:1.8;
                                color:#374151;
                                white-space:normal;
                            ">
                                {formatted_body}
                            </div>

                            {action_section}
                        </div>
                    </div>

                    <!-- INFO STRIP -->
                    <div style="padding:8px 24px 0 24px;">
                        <div style="
                            background:#eff6ff;
                            border:1px solid #bfdbfe;
                            border-radius:12px;
                            padding:12px 14px;
                            color:#1e40af;
                            font-size:12px;
                            line-height:1.6;
                        ">
                            This email was generated by Trade-AI to deliver an automated trading update, including strategy signals, market triggers, and system-generated notifications.
                        </div>
                    </div>

                    <!-- FOOTER -->
                    <div style="padding:20px 24px 24px 24px; text-align:center;">
                        <div style="
                            border-top:1px solid #e5e7eb;
                            padding-top:14px;
                            font-size:12px;
                            color:#6b7280;
                            line-height:1.6;
                        ">
                            TRADE AI APP @2026 | Powered by IrfanA
                        </div>
                    </div>

                </div>
            </div>
        </body>
    </html>
    """.strip()


def _attach_inline_logo(msg: EmailMessage, logo_path: Path) -> bool:
    if not logo_path.exists():
        return False

    mime_type, _ = mimetypes.guess_type(str(logo_path))
    if mime_type:
        maintype, subtype = mime_type.split("/", 1)
    else:
        maintype, subtype = "image", "png"

    with open(logo_path, "rb") as f:
        logo_data = f.read()

    msg.get_payload()[-1].add_related(
        logo_data,
        maintype=maintype,
        subtype=subtype,
        cid="<tradeai_logo>",
        filename=logo_path.name,
        disposition="inline",
    )
    return True


def send_email(
    to_email: str | list[str],
    subject: str,
    body: str,
    title: str | None = None,
    action_link: str | None = None,
) -> None:
    recipients = _normalize_recipients(to_email)

    if not recipients:
        raise ValueError("to_email is empty.")

    if not SMTP_HOST:
        raise ValueError("SMTP_HOST is not set.")
    if not SMTP_USER:
        raise ValueError("SMTP_USER is not set.")
    if not SMTP_PASSWORD:
        raise ValueError("SMTP_PASSWORD is not set.")
    if not MAIL_FROM:
        raise ValueError("MAIL_FROM is not set.")

    plain_body = _build_plain_template(
        body=body,
        title=title,
        action_link=action_link,
    )

    html_body = _build_html_template(
        body=body,
        title=title,
        include_logo=LOGO_PATH.exists(),
        action_link=action_link,
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = "undisclosed-recipients:;"
    msg["Bcc"] = ", ".join(recipients)

    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")

    _attach_inline_logo(msg, LOGO_PATH)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    print(f"[📩] Mail has been sent to: {recipients}")