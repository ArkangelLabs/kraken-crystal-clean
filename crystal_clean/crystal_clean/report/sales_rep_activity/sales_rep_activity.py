# Copyright (c) 2025, Crystal Clean Maintenance and contributors
# For license information, please see license.txt

import frappe
from frappe import _


def execute(filters=None):
	columns = get_columns()
	data = get_data(filters)
	return columns, data


def get_columns():
	return [
		{
			"fieldname": "sales_rep",
			"label": _("Sales Rep"),
			"fieldtype": "Data",
			"width": 200,
		},
		{
			"fieldname": "total_contracts",
			"label": _("Total Contracts"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "modified_today",
			"label": _("Modified Today"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "modified_this_week",
			"label": _("Modified This Week"),
			"fieldtype": "Int",
			"width": 130,
		},
		{
			"fieldname": "modified_this_month",
			"label": _("Modified This Month"),
			"fieldtype": "Int",
			"width": 140,
		},
		{
			"fieldname": "last_activity",
			"label": _("Last Activity"),
			"fieldtype": "Datetime",
			"width": 160,
		},
	]


def get_data(filters):
	return frappe.db.sql(
		"""
		SELECT
			COALESCE(custom_sales_rep, 'Unassigned') as sales_rep,
			COUNT(*) as total_contracts,
			SUM(CASE WHEN DATE(custom_last_aspire_sync) = CURDATE() THEN 1 ELSE 0 END) as modified_today,
			SUM(CASE WHEN custom_last_aspire_sync >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) as modified_this_week,
			SUM(CASE WHEN custom_last_aspire_sync >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN 1 ELSE 0 END) as modified_this_month,
			MAX(custom_last_aspire_sync) as last_activity
		FROM `tabOpportunity`
		WHERE custom_sales_rep IS NOT NULL
			AND custom_sales_rep != ''
		GROUP BY custom_sales_rep
		ORDER BY last_activity DESC
		""",
		as_dict=True,
	)
