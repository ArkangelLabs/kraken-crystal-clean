# Copyright (c) 2024, Crystal Clean Maintenance and contributors
# For license information, please see license.txt

"""
Data transformation layer for Aspire API data.

Transforms Aspire API responses (PascalCase) to Frappe DocType format (snake_case).
"""

from datetime import datetime


def pascal_to_snake(name):
	"""Convert PascalCase to snake_case."""
	import re

	s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
	return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def parse_date(date_str):
	"""Parse ISO date string to date object."""
	if not date_str:
		return None
	try:
		# Handle ISO format: 2024-01-15T00:00:00Z
		return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
	except (ValueError, AttributeError):
		return None


def parse_datetime(dt_str):
	"""Parse ISO datetime string to datetime object (without timezone for MariaDB)."""
	if not dt_str:
		return None
	try:
		dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
		# Strip timezone and microseconds for MariaDB compatibility
		return dt.replace(tzinfo=None, microsecond=0)
	except (ValueError, AttributeError):
		return None


def transform_company_to_customer(aspire_company):
	"""Transform Aspire Company to Frappe Customer."""
	return {
		"doctype": "Customer",
		"customer_name": aspire_company.get("CompanyName"),
		"customer_type": "Company",
		"disabled": not aspire_company.get("Active", True),
		"custom_aspire_company_id": aspire_company.get("CompanyID"),
		"custom_last_aspire_sync": datetime.now(),
	}


def clean_phone(phone_str):
	"""Clean phone number - remove extensions, trailing text, and invalid characters.

	Handles cases like:
	- "506-284-097 not in servi" -> "506-284-097"
	- "1-952-947-0007 E" -> "1-952-947-0007"
	- "902-579-3084 ext 123" -> "902-579-3084"
	"""
	if not phone_str:
		return None
	import re

	# Remove extension markers and everything after (x, ext, EXT, or any letter)
	phone = re.split(r'\s*[xX]|ext|EXT|\s+[a-zA-Z]', phone_str)[0].strip()

	# Keep only valid phone characters (digits, +, -, (, ), spaces)
	cleaned = re.sub(r'[^\d+\-() ]', '', phone)

	# Must have at least 7 digits to be valid
	digits_only = re.sub(r'\D', '', cleaned)
	if len(digits_only) >= 7:
		return cleaned.strip()

	return None


def transform_contact(aspire_contact, customer_name=None):
	"""Transform Aspire Contact to Frappe Contact."""
	first_name = aspire_contact.get("FirstName", "")
	last_name = aspire_contact.get("LastName", "")

	contact = {
		"doctype": "Contact",
		"first_name": first_name,
		"last_name": last_name,
		"status": "Open" if aspire_contact.get("Active", True) else "Passive",
		"custom_aspire_contact_id": aspire_contact.get("ContactID"),
		"custom_last_aspire_sync": datetime.now(),
	}

	# Email
	email = aspire_contact.get("Email")
	if email:
		contact["email_ids"] = [{"email_id": email, "is_primary": 1}]

	# Phone numbers (cleaned of extensions)
	phone_nos = []
	mobile = clean_phone(aspire_contact.get("MobilePhone"))
	office = clean_phone(aspire_contact.get("OfficePhone"))
	if mobile:
		phone_nos.append({"phone": mobile, "is_primary_mobile_no": 1})
	if office:
		phone_nos.append({"phone": office, "is_primary_phone": 1})
	if phone_nos:
		contact["phone_nos"] = phone_nos

	# Link to customer
	if customer_name:
		contact["links"] = [{"link_doctype": "Customer", "link_name": customer_name}]

	return contact


def transform_property_to_service_property(aspire_property, customer_name=None):
	"""Transform Aspire Property to Service Property DocType."""
	# Map PropertyStatusName to status
	status_map = {"Customer": "Customer", "Prospect": "Prospect"}
	status = status_map.get(aspire_property.get("PropertyStatusName"), "Inactive")

	property_id = aspire_property.get("PropertyID")

	# Build unique property name - append city if available to avoid duplicates
	# e.g., "Cyber Centre" becomes "Cyber Centre - Halifax"
	base_name = aspire_property.get("PropertyName", "")
	city = aspire_property.get("PropertyAddressCity")
	if city and city.strip():
		property_name = f"{base_name} - {city.strip()}"
	else:
		property_name = base_name

	# Don't set 'name' - let Frappe handle naming via autoname or use property_name
	return {
		"doctype": "Service Property",
		"property_name": property_name,
		"customer": customer_name,
		"property_status_name": status,
		"industry_name": aspire_property.get("IndustryName"),
		"budget": aspire_property.get("Budget"),
		"property_address_line1": aspire_property.get("PropertyAddressLine1"),
		"property_address_city": aspire_property.get("PropertyAddressCity"),
		"property_address_state_province_code": aspire_property.get("PropertyAddressStateProvinceCode"),
		"property_address_zip_code": aspire_property.get("PropertyAddressZipCode"),
		"geo_location_latitude": aspire_property.get("GEOLocationLatitude"),
		"geo_location_longitude": aspire_property.get("GEOLocationLongitude"),
		"account_owner_contact_name": aspire_property.get("AccountOwnerContactName"),
		"aspire_property_id": property_id,
		"last_aspire_sync": datetime.now(),
	}


def transform_opportunity(aspire_opportunity, customer_name=None, service_property_name=None):
	"""Transform Aspire Opportunity to Frappe Opportunity with custom fields."""
	# Map status
	status_name = aspire_opportunity.get("OpportunityStatusName", "")
	if "Won" in status_name:
		status = "Converted"
	elif "Lost" in status_name:
		status = "Lost"
	else:
		status = "Open"

	return {
		"doctype": "Opportunity",
		"opportunity_from": "Customer" if customer_name else "Lead",
		"party_name": customer_name,
		"status": status,
		"custom_aspire_opportunity_id": aspire_opportunity.get("OpportunityID"),
		"custom_service_property": service_property_name,
		"custom_renewal_date": parse_date(aspire_opportunity.get("RenewalDate")),
		"custom_estimated_dollars": aspire_opportunity.get("EstimatedDollars"),
		"custom_estimated_gross_margin": aspire_opportunity.get("EstimatedGrossMarginDollars"),
		"custom_sales_rep": aspire_opportunity.get("SalesRepContactName"),
		"custom_branch": aspire_opportunity.get("BranchName"),
		"custom_division": aspire_opportunity.get("DivisionName"),
		"custom_opportunity_type": aspire_opportunity.get("OpportunityType"),
		"custom_won_date": parse_date(aspire_opportunity.get("WonDate")),
		"custom_last_aspire_sync": datetime.now(),
		"custom_aspire_modified_date": parse_datetime(aspire_opportunity.get("ModifiedDate")),
	}


def transform_work_ticket(aspire_ticket, service_property_name=None, opportunity_name=None):
	"""Transform Aspire Work Ticket to Work Ticket DocType."""
	# Map WorkTicketStatusName to status
	status_map = {
		"Scheduled": "Scheduled",
		"In Progress": "In Progress",
		"Complete": "Complete",
		"Cancelled": "Cancelled",
	}
	status = status_map.get(aspire_ticket.get("WorkTicketStatusName"), "Scheduled")

	return {
		"doctype": "Work Ticket",
		"work_ticket_number": aspire_ticket.get("WorkTicketNumber"),
		"work_ticket_status_name": status,
		"service_property": service_property_name,
		"scheduled_start_date": parse_date(aspire_ticket.get("ScheduledStartDate")),
		"complete_date": parse_date(aspire_ticket.get("CompletedDate")),
		"hours_est": aspire_ticket.get("HoursEstimated"),
		"hours_act": aspire_ticket.get("HoursActual"),
		"labor_cost_act": aspire_ticket.get("ActualLaborCost"),
		"material_cost_act": aspire_ticket.get("ActualMaterialCost"),
		"equipment_cost_act": aspire_ticket.get("ActualEquipmentCost"),
		"earned_revenue": aspire_ticket.get("EarnedRevenue") or aspire_ticket.get("ActualRevenue"),
		"crew_leader_name": aspire_ticket.get("CrewLeaderName"),
		"aspire_work_ticket_id": aspire_ticket.get("WorkTicketID"),
		"aspire_opportunity_service_id": aspire_ticket.get("OpportunityServiceID"),
		"last_aspire_sync": datetime.now(),
	}


def transform_batch(records, transform_func, **kwargs):
	"""Transform a batch of records, returning transformed records and errors."""
	transformed = []
	errors = []

	for record in records:
		try:
			result = transform_func(record, **kwargs)
			transformed.append(result)
		except Exception as e:
			errors.append({"record": record, "error": str(e)})

	return transformed, errors


# =============================================================================
# NEW ASPIRE DOCTYPES - Clean slate transformations
# =============================================================================


def transform_to_aspire_company(aspire_company):
	"""Transform Aspire Company to Aspire Company DocType (clean, direct mapping)."""
	return {
		"doctype": "Aspire Company",
		"company_name": aspire_company.get("CompanyName"),
		"aspire_company_id": aspire_company.get("CompanyID"),
		"active": 1 if aspire_company.get("Active", True) else 0,
		"last_aspire_sync": datetime.now(),
	}


def transform_to_aspire_contact(aspire_contact, company_name=None):
	"""Transform Aspire Contact to Aspire Contact DocType (clean, direct mapping)."""
	return {
		"doctype": "Aspire Contact",
		"first_name": aspire_contact.get("FirstName") or "",
		"last_name": aspire_contact.get("LastName") or "",
		"email": aspire_contact.get("Email"),
		"mobile_phone": clean_phone(aspire_contact.get("MobilePhone")),
		"office_phone": clean_phone(aspire_contact.get("OfficePhone")),
		"company": company_name,
		"active": 1 if aspire_contact.get("Active", True) else 0,
		"aspire_contact_id": aspire_contact.get("ContactID"),
		"last_aspire_sync": datetime.now(),
	}


def transform_to_aspire_contract(aspire_opportunity, company_name=None, property_name=None):
	"""Transform Aspire Opportunity to Aspire Contract DocType (clean, direct mapping).

	Only syncs contracts where:
	- OpportunityType = 'Contract'
	- OpportunityStatusName != 'Draft'
	- RenewalDate is set
	"""
	# Map status directly
	status_name = aspire_opportunity.get("OpportunityStatusName", "")
	if "Won" in status_name:
		contract_status = "Won"
	elif "Lost" in status_name:
		contract_status = "Lost"
	else:
		contract_status = "Open"

	return {
		"doctype": "Aspire Contract",
		"company": company_name,
		"property": property_name,
		"contract_status": contract_status,
		"renewal_date": parse_date(aspire_opportunity.get("RenewalDate")),
		"sales_rep": aspire_opportunity.get("SalesRepContactName"),
		"estimated_value": aspire_opportunity.get("EstimatedDollars"),
		"gross_margin": aspire_opportunity.get("EstimatedGrossMarginDollars"),
		"branch": aspire_opportunity.get("BranchName"),
		"division": aspire_opportunity.get("DivisionName"),
		"won_date": parse_date(aspire_opportunity.get("WonDate")),
		"aspire_modified_date": parse_datetime(aspire_opportunity.get("ModifiedDate")),
		"aspire_opportunity_id": aspire_opportunity.get("OpportunityID"),
		"last_aspire_sync": datetime.now(),
	}
