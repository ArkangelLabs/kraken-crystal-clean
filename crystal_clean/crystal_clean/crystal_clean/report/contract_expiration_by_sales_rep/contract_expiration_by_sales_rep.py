# Copyright (c) 2025, Crystal Clean Maintenance and contributors
# For license information, please see license.txt

import frappe
from frappe import _


def execute(filters=None):
	columns = get_columns()
	data = get_data(filters)
	chart = get_chart_data(data)
	return columns, data, None, chart


def get_columns():
	return [
		{
			"fieldname": "sales_rep",
			"label": _("Sales Rep"),
			"fieldtype": "Data",
			"width": 300,
		},
		{
			"fieldname": "expires_30_days",
			"label": _("Expiring in 30 Days"),
			"fieldtype": "Int",
			"width": 200,
		},
		{
			"fieldname": "expires_60_days",
			"label": _("Expiring in 60 Days"),
			"fieldtype": "Int",
			"width": 200,
		},
		{
			"fieldname": "expires_90_days",
			"label": _("Expiring in 90 Days"),
			"fieldtype": "Int",
			"width": 200,
		},
	]


def get_data(filters):
	return frappe.db.sql(
		"""
		SELECT
			COALESCE(sales_rep, 'Unassigned') as sales_rep,
			SUM(CASE
				WHEN renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY)
				THEN 1 ELSE 0
			END) as expires_30_days,
			SUM(CASE
				WHEN renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 60 DAY)
				THEN 1 ELSE 0
			END) as expires_60_days,
			SUM(CASE
				WHEN renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 90 DAY)
				THEN 1 ELSE 0
			END) as expires_90_days
		FROM `tabAspire Contract`
		WHERE renewal_date IS NOT NULL
			AND renewal_date >= CURDATE()
		GROUP BY sales_rep
		HAVING expires_30_days > 0 OR expires_60_days > 0 OR expires_90_days > 0
		ORDER BY expires_30_days DESC, expires_60_days DESC
		""",
		as_dict=True,
	)


def get_chart_data(data):
	if not data:
		return None

	labels = [row.get("sales_rep") for row in data]

	return {
		"data": {
			"labels": labels,
			"datasets": [
				{
					"name": "30 Days",
					"values": [row.get("expires_30_days", 0) for row in data],
				},
				{
					"name": "60 Days",
					"values": [row.get("expires_60_days", 0) for row in data],
				},
				{
					"name": "90 Days",
					"values": [row.get("expires_90_days", 0) for row in data],
				},
			],
		},
		"type": "bar",
		"colors": ["#ff5858", "#ff9f43", "#ffc107"],
		"barOptions": {"stacked": False},
	}
