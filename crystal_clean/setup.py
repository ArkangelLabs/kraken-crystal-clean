import frappe
from frappe import _


# Email configuration (non-sensitive)
SMTP_SERVER = "email-smtp.us-east-1.amazonaws.com"
SMTP_PORT = 587
SMTP_LOGIN = "AKIA4E4ZWFXVEFZ7Z57M"
EMAIL_DOMAIN = "mythril.cloud"
AUTO_EMAIL_ID = "notifications@mythril.cloud"


def after_install():
    """Run after app installation."""
    create_email_domain()
    create_email_account()
    frappe.db.commit()


def create_email_domain():
    """Create Email Domain for mythril.cloud."""
    if frappe.db.exists("Email Domain", EMAIL_DOMAIN):
        return

    frappe.get_doc({
        "doctype": "Email Domain",
        "domain_name": EMAIL_DOMAIN,
        "email_server": "",
        "use_imap": 0,
        "smtp_server": SMTP_SERVER,
        "smtp_port": SMTP_PORT,
        "use_tls": 1,
        "use_ssl": 0
    }).insert(ignore_permissions=True)

    frappe.logger().info(f"Created Email Domain: {EMAIL_DOMAIN}")


def create_email_account():
    """Create default Email Account for notifications."""
    account_name = "Crystal Clean Notifications"

    if frappe.db.exists("Email Account", account_name):
        return

    # Password must be set in site_config
    mail_password = frappe.conf.get("mail_password")
    if not mail_password:
        frappe.logger().warning(
            "Email Account not created: mail_password not set in site_config. "
            "Add mail_password to site configuration to enable email."
        )
        return

    frappe.get_doc({
        "doctype": "Email Account",
        "email_account_name": account_name,
        "email_id": AUTO_EMAIL_ID,
        "domain": EMAIL_DOMAIN,
        "enable_outgoing": 1,
        "default_outgoing": 1,
        "enable_incoming": 0,
        "smtp_server": SMTP_SERVER,
        "smtp_port": SMTP_PORT,
        "use_tls": 1,
        "use_ssl_for_outgoing": 0,
        "login_id_is_different": 1,
        "login_id": SMTP_LOGIN,
        "password": mail_password,
        "send_unsubscribe_message": 1,
        "track_email_status": 1
    }).insert(ignore_permissions=True)

    frappe.logger().info(f"Created Email Account: {account_name}")
