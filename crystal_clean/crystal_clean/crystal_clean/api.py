# Copyright (c) 2025, Crystal Clean and contributors
# For license information, please see license.txt

import frappe
from frappe import _


@frappe.whitelist()
def create_issue_from_contract(contract_name):
	"""Create an Issue Process document from an Aspire Contract.

	Prepares the issue for future sync to Aspire Activities API.
	"""
	contract = frappe.get_doc("Aspire Contract", contract_name)

	# Calculate expiry bucket
	days_until = None
	expiry_bucket = None
	if contract.renewal_date:
		from datetime import date
		days_until = (contract.renewal_date - date.today()).days
		if days_until <= 30:
			expiry_bucket = "0-30 Days"
		elif days_until <= 60:
			expiry_bucket = "31-60 Days"
		elif days_until <= 90:
			expiry_bucket = "61-90 Days"
		else:
			expiry_bucket = "90+ Days"

	# Create Issue Process
	issue = frappe.new_doc("Issue Process")
	issue.subject = "contract ending"
	issue.status = "Open"
	issue.priority = "High" if days_until and days_until <= 30 else "Medium"
	issue.due_date = contract.renewal_date
	issue.contract = contract_name
	issue.contract_name = contract.company
	issue.customer = contract.company
	issue.sales_rep = contract.sales_rep
	issue.end_date = contract.renewal_date
	issue.expiry_bucket = expiry_bucket
	issue.aspire_opportunity_id = contract.aspire_opportunity_id or 0
	issue.aspire_sync_status = "Not Synced"
	issue.description = f"""Contract expiring for {contract.company}

Property: {contract.property or 'N/A'}
Renewal Date: {contract.renewal_date}
Estimated Value: {contract.estimated_value or 0}
Sales Rep: {contract.sales_rep or 'Unassigned'}
Days Until Expiry: {days_until}
"""

	issue.insert()
	frappe.db.commit()

	return issue.name


@frappe.whitelist()
def send_renewal_email(issue_name):
	"""Send sales renewal email for an Issue Process."""
	from datetime import timedelta

	issue = frappe.get_doc("Issue Process", issue_name)
	contract = frappe.get_doc("Aspire Contract", issue.contract)

	# Calculate renewal target (due date - 30 days)
	renewal_target = None
	if issue.due_date:
		renewal_target = issue.due_date - timedelta(days=30)

	# Hardcoded recipient for testing
	recipient = "pier-luc@mythril.systems"

	subject = f"Issue #{issue.name} - Sales Renewal - Re: Contract #{issue.contract}"

	message = f"""
	<p><strong>Issue#:</strong> {issue.name}</p>
	<p><strong>Contract#:</strong> {issue.contract}</p>
	<p><strong>Contract Due Date:</strong> {issue.due_date}</p>
	<br>
	<p><strong>Contract Renewal Target:</strong> {renewal_target} <span style="background-color: yellow;">&lt;-- Contract Due Date - 30 Days</span></p>
	<br>
	<p><strong>Property:</strong> {contract.property or 'N/A'}</p>
	<p><strong>Estimated Value:</strong> ${contract.estimated_value or 0:,.2f}</p>
	"""

	frappe.sendmail(
		recipients=[recipient],
		subject=subject,
		message=message,
		now=True
	)

	return {"success": True, "recipient": recipient}
