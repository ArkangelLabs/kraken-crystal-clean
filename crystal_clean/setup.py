import frappe
from frappe import _


def after_install():
    """Run after app installation."""
    create_email_domain()
    create_email_account()
    frappe.db.commit()


def create_email_domain():
    """Create Email Domain for mythril.cloud."""
    if frappe.db.exists("Email Domain", "mythril.cloud"):
        return

    frappe.get_doc({
        "doctype": "Email Domain",
        "domain_name": "mythril.cloud",
        "email_server": "",
        "use_imap": 0,
        "smtp_server": frappe.conf.get("mail_server", "email-smtp.us-east-1.amazonaws.com"),
        "smtp_port": frappe.conf.get("mail_port", 587),
        "use_tls": 1,
        "use_ssl": 0
    }).insert(ignore_permissions=True)
    
    frappe.logger().info("Created Email Domain: mythril.cloud")


def create_email_account():
    """Create default Email Account for notifications."""
    account_name = "Crystal Clean Notifications"
    
    if frappe.db.exists("Email Account", account_name):
        return

    email_id = frappe.conf.get("auto_email_id", "notifications@mythril.cloud")
    
    frappe.get_doc({
        "doctype": "Email Account",
        "email_account_name": account_name,
        "email_id": email_id,
        "domain": "mythril.cloud",
        "enable_outgoing": 1,
        "default_outgoing": 1,
        "enable_incoming": 0,
        "smtp_server": frappe.conf.get("mail_server", "email-smtp.us-east-1.amazonaws.com"),
        "smtp_port": frappe.conf.get("mail_port", 587),
        "use_tls": 1,
        "use_ssl_for_outgoing": 0,
        "login_id_is_different": 1,
        "login_id": frappe.conf.get("mail_login"),
        "password": frappe.conf.get("mail_password"),
        "send_unsubscribe_message": 1,
        "track_email_status": 1
    }).insert(ignore_permissions=True)
    
    frappe.logger().info(f"Created Email Account: {account_name}")
