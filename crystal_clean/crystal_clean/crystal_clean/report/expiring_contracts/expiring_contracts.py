# Copyright (c) 2025, Crystal Clean and contributors
# For license information, please see license.txt

import frappe
from frappe import _


def execute(filters=None):
	columns = get_columns()
	data = get_data(filters)
	chart = get_chart_data()
	return columns, data, None, chart


def get_columns():
	return [
		{
			"fieldname": "sales_rep",
			"label": _("Sales Rep"),
			"fieldtype": "Data",
			"width": 180,
		},
		{
			"fieldname": "company",
			"label": _("Company"),
			"fieldtype": "Link",
			"options": "Aspire Company",
			"width": 200,
		},
		{
			"fieldname": "property",
			"label": _("Property"),
			"fieldtype": "Link",
			"options": "Service Property",
			"width": 200,
		},
		{
			"fieldname": "renewal_date",
			"label": _("Renewal Date"),
			"fieldtype": "Date",
			"width": 120,
		},
		{
			"fieldname": "estimated_value",
			"label": _("Value"),
			"fieldtype": "Currency",
			"width": 120,
		},
		{
			"fieldname": "days_until_expiry",
			"label": _("Days Until Expiry"),
			"fieldtype": "Int",
			"width": 140,
		},
		{
			"fieldname": "contract_status",
			"label": _("Status"),
			"fieldtype": "Data",
			"width": 100,
		},
		{
			"fieldname": "name",
			"label": _("Contract"),
			"fieldtype": "Link",
			"options": "Aspire Contract",
			"width": 150,
		},
		{
			"fieldname": "action",
			"label": _("Action"),
			"fieldtype": "Data",
			"width": 120,
		},
	]


def get_data(filters):
	conditions = ["renewal_date IS NOT NULL", "renewal_date >= CURDATE()"]
	values = {}

	# Apply filters
	if filters:
		if filters.get("sales_rep"):
			conditions.append("sales_rep = %(sales_rep)s")
			values["sales_rep"] = filters.get("sales_rep")

		if filters.get("from_date"):
			conditions.append("renewal_date >= %(from_date)s")
			values["from_date"] = filters.get("from_date")

		if filters.get("to_date"):
			conditions.append("renewal_date <= %(to_date)s")
			values["to_date"] = filters.get("to_date")

		if filters.get("days"):
			conditions.append("renewal_date <= DATE_ADD(CURDATE(), INTERVAL %(days)s DAY)")
			values["days"] = int(filters.get("days"))

	where_clause = " AND ".join(conditions)

	return frappe.db.sql(
		f"""
		SELECT
			name,
			COALESCE(sales_rep, 'Unassigned') as sales_rep,
			company,
			property,
			renewal_date,
			estimated_value,
			DATEDIFF(renewal_date, CURDATE()) as days_until_expiry,
			contract_status,
			aspire_opportunity_id
		FROM `tabAspire Contract`
		WHERE {where_clause}
		ORDER BY renewal_date ASC
		""",
		values,
		as_dict=True,
	)


def get_chart_data():
	"""Generate bar chart data for contracts expiring in 30/60/90/180 day buckets."""
	buckets = frappe.db.sql(
		"""
		SELECT
			CASE
				WHEN DATEDIFF(renewal_date, CURDATE()) BETWEEN 0 AND 30 THEN '0-30 Days'
				WHEN DATEDIFF(renewal_date, CURDATE()) BETWEEN 31 AND 60 THEN '31-60 Days'
				WHEN DATEDIFF(renewal_date, CURDATE()) BETWEEN 61 AND 90 THEN '61-90 Days'
				WHEN DATEDIFF(renewal_date, CURDATE()) BETWEEN 91 AND 180 THEN '91-180 Days'
			END as period,
			COUNT(*) as count,
			SUM(COALESCE(estimated_value, 0)) as total_value
		FROM `tabAspire Contract`
		WHERE renewal_date IS NOT NULL
			AND renewal_date >= CURDATE()
			AND renewal_date <= DATE_ADD(CURDATE(), INTERVAL 180 DAY)
		GROUP BY period
		ORDER BY
			CASE period
				WHEN '0-30 Days' THEN 1
				WHEN '31-60 Days' THEN 2
				WHEN '61-90 Days' THEN 3
				WHEN '91-180 Days' THEN 4
			END
		""",
		as_dict=True,
	)

	# Ensure all buckets exist even if empty
	period_order = ["0-30 Days", "31-60 Days", "61-90 Days", "91-180 Days"]
	bucket_map = {b["period"]: b for b in buckets if b.get("period")}

	labels = []
	counts = []
	for period in period_order:
		labels.append(period)
		counts.append(bucket_map.get(period, {}).get("count", 0))

	return {
		"data": {
			"labels": labels,
			"datasets": [{"name": _("Contracts"), "values": counts}],
		},
		"type": "bar",
		"colors": ["#fc4f51", "#ff7846", "#ffc107", "#5e64ff"],
	}
