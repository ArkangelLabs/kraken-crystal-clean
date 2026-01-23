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
			"width": 250,
		},
		{
			"fieldname": "total_contracts",
			"label": _("Total Contracts"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "exp_30d",
			"label": _("Exp 30 Days"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "exp_60d",
			"label": _("Exp 60 Days"),
			"fieldtype": "Int",
			"width": 120,
		},
		{
			"fieldname": "exp_90d",
			"label": _("Exp 90 Days"),
			"fieldtype": "Int",
			"width": 120,
		},
	]


def get_data(filters):
	return frappe.db.sql(
		"""
		SELECT
			COALESCE(custom_sales_rep, 'Unassigned') as sales_rep,
			COUNT(*) as total_contracts,
			SUM(CASE
				WHEN custom_renewal_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY)
				THEN 1 ELSE 0
			END) as exp_30d,
			SUM(CASE
				WHEN custom_renewal_date BETWEEN DATE_ADD(CURDATE(), INTERVAL 31 DAY) AND DATE_ADD(CURDATE(), INTERVAL 60 DAY)
				THEN 1 ELSE 0
			END) as exp_60d,
			SUM(CASE
				WHEN custom_renewal_date BETWEEN DATE_ADD(CURDATE(), INTERVAL 61 DAY) AND DATE_ADD(CURDATE(), INTERVAL 90 DAY)
				THEN 1 ELSE 0
			END) as exp_90d
		FROM `tabOpportunity`
		WHERE custom_renewal_date IS NOT NULL
			AND custom_renewal_date >= CURDATE()
			AND custom_renewal_date <= DATE_ADD(CURDATE(), INTERVAL 90 DAY)
			AND custom_sales_rep IS NOT NULL
			AND custom_sales_rep != ''
		GROUP BY custom_sales_rep
		ORDER BY total_contracts DESC
		""",
		as_dict=True,
	)
