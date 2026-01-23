app_name = "crystal_clean"
app_title = "Crystal Clean"
app_publisher = "Crystal Clean Maintenance"
app_description = "Aspire CRM Integration"
app_email = "admin@becrystalclean.com"
app_license = "mit"

# Apps
# ------------------

# No required apps - this is a standalone Frappe app

# Fixtures
# Fixtures are exported/imported during bench migrate
# - Custom Field: Custom fields added to standard DocTypes (e.g., Opportunity)
# - Workspace Sidebar, Number Card, etc.: Dashboard components
# - Email Domain/Template/Notification: Email configuration
fixtures = [
	{"dt": "Custom Field", "filters": [["module", "=", "Crystal Clean"]]},
	{"dt": "Workspace Sidebar", "filters": [["module", "=", "Crystal Clean"]]},
	{"dt": "Number Card", "filters": [["module", "=", "Crystal Clean"]]},
	{"dt": "Dashboard Chart", "filters": [["module", "=", "Crystal Clean"]]},
	{"dt": "Dashboard", "filters": [["module", "=", "Crystal Clean"]]},
	{"dt": "Email Domain"},
	{"dt": "Email Template"},
	{"dt": "Notification"}
]

# Installation
# ------------

after_install = "crystal_clean.setup.after_install"

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "crystal_clean",
# 		"logo": "/assets/crystal_clean/logo.png",
# 		"title": "Crystal Clean",
# 		"route": "/crystal_clean",
# 		"has_permission": "crystal_clean.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/crystal_clean/css/crystal_clean.css"
# app_include_js = "/assets/crystal_clean/js/crystal_clean.js"

# include js, css files in header of web template
# web_include_css = "/assets/crystal_clean/css/crystal_clean.css"
# web_include_js = "/assets/crystal_clean/js/crystal_clean.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "crystal_clean/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

doctype_list_js = {
	"Contract": "public/js/contract_list.js",
	"Issue Process": "public/js/issue_process_list.js"
}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "crystal_clean/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# automatically load and sync documents of this doctype from downstream apps
# importable_doctypes = [doctype_1]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "crystal_clean.utils.jinja_methods",
# 	"filters": "crystal_clean.utils.jinja_filters"
# }

# Uninstallation
# ------------

# before_uninstall = "crystal_clean.uninstall.before_uninstall"
# after_uninstall = "crystal_clean.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "crystal_clean.utils.before_app_install"
# after_app_install = "crystal_clean.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "crystal_clean.utils.before_app_uninstall"
# after_app_uninstall = "crystal_clean.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "crystal_clean.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

scheduler_events = {
	"cron": {
		"0 1 * * *": ["crystal_clean.integrations.aspire.sync.incremental_sync"]
	}
}

# Testing
# -------

# before_tests = "crystal_clean.install.before_tests"

# Extend DocType Class
# ------------------------------
#
# Specify custom mixins to extend the standard doctype controller.
# extend_doctype_class = {
# 	"Task": "crystal_clean.custom.task.CustomTaskMixin"
# }

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "crystal_clean.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "crystal_clean.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["crystal_clean.utils.before_request"]
# after_request = ["crystal_clean.utils.after_request"]

# Job Events
# ----------
# before_job = ["crystal_clean.utils.before_job"]
# after_job = ["crystal_clean.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"crystal_clean.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

# Translation
# ------------
# List of apps whose translatable strings should be excluded from this app's translations.
# ignore_translatable_strings_from = []
