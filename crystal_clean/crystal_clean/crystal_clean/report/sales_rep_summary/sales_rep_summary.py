# Copyright (c) 2025, Crystal Clean and contributors
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
			"width": 200,
		},
		{
			"fieldname": "total_contracts",
			"label": _("Total Contracts"),
			"fieldtype": "Int",
			"width": 140,
		},
		{
			"fieldname": "expiring_30d",
			"label": _("Expiring 30d"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "expiring_60d",
			"label": _("Expiring 60d"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "expiring_90d",
			"label": _("Expiring 90d"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "total_value",
			"label": _("Total Value"),
			"fieldtype": "Currency",
			"width": 150,
		},
	]


def get_data(filters):
	return frappe.db.sql(
		"""
		SELECT
			COALESCE(sales_rep, 'Unassigned') as sales_rep,
			COUNT(*) as total_contracts,
			SUM(CASE
				WHEN renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY)
				THEN 1 ELSE 0
			END) as expiring_30d,
			SUM(CASE
				WHEN renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 60 DAY)
				THEN 1 ELSE 0
			END) as expiring_60d,
			SUM(CASE
				WHEN renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 90 DAY)
				THEN 1 ELSE 0
			END) as expiring_90d,
			SUM(COALESCE(estimated_value, 0)) as total_value
		FROM `tabAspire Contract`
		WHERE renewal_date IS NOT NULL
			AND renewal_date >= CURDATE()
		GROUP BY sales_rep
		ORDER BY total_contracts DESC
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
					"name": "Total Contracts",
					"values": [row.get("total_contracts", 0) for row in data],
				},
				{
					"name": "Expiring 30d",
					"values": [row.get("expiring_30d", 0) for row in data],
				},
				{
					"name": "Expiring 60d",
					"values": [row.get("expiring_60d", 0) for row in data],
				},
				{
					"name": "Expiring 90d",
					"values": [row.get("expiring_90d", 0) for row in data],
				},
			],
		},
		"type": "bar",
		"colors": ["#4299E1", "#ff5858", "#ff9f43", "#ffc107"],
		"barOptions": {"stacked": False},
	}
