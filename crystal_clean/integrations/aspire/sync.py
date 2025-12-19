# Copyright (c) 2024, Crystal Clean Maintenance and contributors
# For license information, please see license.txt

"""
Sync orchestration for Aspire API integration.

Sync order (respects foreign keys):
1. Companies → Customer
2. Properties → Service Property
3. Contacts → Contact
4. Opportunities → Opportunity
5. Work Tickets → Work Ticket
"""

import json
import time
from datetime import datetime

import frappe

# Batch settings to avoid overwhelming the server
BATCH_SIZE = 100  # Commit after N records
BATCH_DELAY = 0.5  # Seconds between batches

from .client import AspireClient, AspireAPIError
from .transform import (
	transform_company_to_customer,
	transform_contact,
	transform_opportunity,
	transform_property_to_service_property,
	transform_work_ticket,
	# New clean slate transforms
	transform_to_aspire_company,
	transform_to_aspire_contact,
	transform_to_aspire_contract,
)


class SyncStats:
	"""Track sync statistics."""

	def __init__(self):
		self.pulled = 0
		self.created = 0
		self.updated = 0
		self.errors = 0

	def to_dict(self):
		return {
			"records_pulled": self.pulled,
			"records_created": self.created,
			"records_updated": self.updated,
			"errors": self.errors,
		}


def create_sync_log(sync_type, entity_type):
	"""Create a new sync log entry."""
	log = frappe.get_doc(
		{"doctype": "Aspire Sync Log", "sync_type": sync_type, "entity_type": entity_type, "status": "Running"}
	)
	log.insert(ignore_permissions=True)
	frappe.db.commit()
	return log.name


def update_sync_log(log_name, status, stats, errors=None, duration=None):
	"""Update sync log with results."""
	log = frappe.get_doc("Aspire Sync Log", log_name)
	log.status = status
	log.records_pulled = stats.pulled
	log.records_created = stats.created
	log.records_updated = stats.updated
	log.errors = stats.errors
	log.completed_at = datetime.now()
	if duration:
		log.sync_duration_seconds = duration
	if errors:
		log.error_details = json.dumps(errors, indent=2, default=str)
	log.save(ignore_permissions=True)
	frappe.db.commit()


def get_last_sync_date(entity_type="All"):
	"""Get the last successful sync date."""
	filters = {"status": "Success"}
	if entity_type != "All":
		filters["entity_type"] = entity_type

	last_sync = frappe.get_all(
		"Aspire Sync Log", filters=filters, fields=["completed_at"], order_by="completed_at desc", limit=1
	)

	if last_sync:
		return last_sync[0].completed_at
	return None


def sync_companies(client, stats, modified_since=None, cutoff_date=None):
	"""Sync companies from Aspire to Customer DocType."""
	errors = []
	companies = client.fetch_companies(modified_since, cutoff_date)
	stats.pulled += len(companies)

	for i, company in enumerate(companies):
		try:
			aspire_id = company.get("CompanyID")
			customer_data = transform_company_to_customer(company)

			# Check if customer exists by aspire_company_id
			existing = frappe.db.get_value("Customer", {"custom_aspire_company_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Customer", existing)
				doc.update(customer_data)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(customer_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Company", "id": company.get("CompanyID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def sync_properties(client, stats, modified_since=None, cutoff_date=None):
	"""Sync properties from Aspire to Service Property DocType."""
	errors = []
	properties = client.fetch_properties(modified_since, cutoff_date)
	stats.pulled += len(properties)

	# Build company ID to customer name mapping
	company_map = {}
	customers = frappe.get_all("Customer", fields=["name", "custom_aspire_company_id"])
	for c in customers:
		if c.custom_aspire_company_id:
			company_map[c.custom_aspire_company_id] = c.name

	for i, prop in enumerate(properties):
		try:
			aspire_id = prop.get("PropertyID")
			company_id = prop.get("CompanyID")
			customer_name = company_map.get(company_id)

			property_data = transform_property_to_service_property(prop, customer_name)

			# Check if property exists
			existing = frappe.db.get_value("Service Property", {"aspire_property_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Service Property", existing)
				doc.update(property_data)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(property_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Property", "id": prop.get("PropertyID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def sync_contacts(client, stats, modified_since=None, cutoff_date=None):
	"""Sync contacts from Aspire to Contact DocType."""
	errors = []
	contacts = client.fetch_contacts(modified_since, cutoff_date)
	stats.pulled += len(contacts)

	# Build company ID to customer name mapping
	company_map = {}
	customers = frappe.get_all("Customer", fields=["name", "custom_aspire_company_id"])
	for c in customers:
		if c.custom_aspire_company_id:
			company_map[c.custom_aspire_company_id] = c.name

	for i, contact in enumerate(contacts):
		try:
			aspire_id = contact.get("ContactID")
			company_id = contact.get("CompanyID")
			customer_name = company_map.get(company_id)

			contact_data = transform_contact(contact, customer_name)

			# Check if contact exists
			existing = frappe.db.get_value("Contact", {"custom_aspire_contact_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Contact", existing)
				# Update basic fields, not child tables
				for key in ["first_name", "last_name", "status", "custom_last_aspire_sync"]:
					if key in contact_data:
						setattr(doc, key, contact_data[key])
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(contact_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Contact", "id": contact.get("ContactID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def sync_opportunities(client, stats, modified_since=None, cutoff_date=None):
	"""Sync opportunities from Aspire to Opportunity DocType."""
	errors = []
	opportunities = client.fetch_opportunities(modified_since, cutoff_date)
	stats.pulled += len(opportunities)

	# Build mappings
	company_map = {}
	customers = frappe.get_all("Customer", fields=["name", "custom_aspire_company_id"])
	for c in customers:
		if c.custom_aspire_company_id:
			company_map[c.custom_aspire_company_id] = c.name

	property_map = {}
	properties = frappe.get_all("Service Property", fields=["name", "aspire_property_id"])
	for p in properties:
		if p.aspire_property_id:
			property_map[p.aspire_property_id] = p.name

	for i, opp in enumerate(opportunities):
		try:
			aspire_id = opp.get("OpportunityID")
			company_id = opp.get("BillingCompanyID") or opp.get("CompanyID")
			property_id = opp.get("PropertyID")

			customer_name = company_map.get(company_id)
			property_name = property_map.get(property_id)

			# Skip if no customer - Frappe Opportunity requires party_name
			if not customer_name:
				continue

			opp_data = transform_opportunity(opp, customer_name, property_name)

			# Check if opportunity exists
			existing = frappe.db.get_value("Opportunity", {"custom_aspire_opportunity_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Opportunity", existing)
				for key, value in opp_data.items():
					if key != "doctype":
						setattr(doc, key, value)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(opp_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Opportunity", "id": opp.get("OpportunityID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def sync_work_tickets(client, stats, modified_since=None, cutoff_date=None):
	"""Sync work tickets from Aspire to Work Ticket DocType."""
	errors = []
	tickets = client.fetch_work_tickets(modified_since, cutoff_date)
	stats.pulled += len(tickets)

	# Build property mapping
	property_map = {}
	properties = frappe.get_all("Service Property", fields=["name", "aspire_property_id"])
	for p in properties:
		if p.aspire_property_id:
			property_map[p.aspire_property_id] = p.name

	for i, ticket in enumerate(tickets):
		try:
			aspire_id = ticket.get("WorkTicketID")
			property_id = ticket.get("PropertyID")
			property_name = property_map.get(property_id)

			ticket_data = transform_work_ticket(ticket, property_name)

			# Check if work ticket exists
			existing = frappe.db.get_value("Work Ticket", {"aspire_work_ticket_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Work Ticket", existing)
				for key, value in ticket_data.items():
					if key not in ["doctype", "naming_series"]:
						setattr(doc, key, value)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(ticket_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "WorkTicket", "id": ticket.get("WorkTicketID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


# =============================================================================
# NEW ASPIRE DOCTYPES - Clean slate sync functions
# =============================================================================


def sync_aspire_companies(client, stats, modified_since=None, cutoff_date=None):
	"""Sync companies from Aspire to Aspire Company DocType."""
	errors = []
	companies = client.fetch_companies(modified_since, cutoff_date)
	stats.pulled += len(companies)

	for i, company in enumerate(companies):
		try:
			aspire_id = company.get("CompanyID")
			company_data = transform_to_aspire_company(company)

			# Check if exists by aspire_company_id
			existing = frappe.db.get_value("Aspire Company", {"aspire_company_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Aspire Company", existing)
				doc.update(company_data)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(company_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Aspire Company", "id": company.get("CompanyID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def sync_aspire_contacts(client, stats, modified_since=None, cutoff_date=None):
	"""Sync contacts from Aspire to Aspire Contact DocType."""
	errors = []
	contacts = client.fetch_contacts(modified_since, cutoff_date)
	stats.pulled += len(contacts)

	# Build company ID to Aspire Company name mapping
	company_map = {}
	aspire_companies = frappe.get_all("Aspire Company", fields=["name", "aspire_company_id"])
	for c in aspire_companies:
		if c.aspire_company_id:
			company_map[c.aspire_company_id] = c.name

	for i, contact in enumerate(contacts):
		try:
			aspire_id = contact.get("ContactID")
			company_id = contact.get("CompanyID")
			company_name = company_map.get(company_id)

			contact_data = transform_to_aspire_contact(contact, company_name)

			# Check if exists by aspire_contact_id
			existing = frappe.db.get_value("Aspire Contact", {"aspire_contact_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Aspire Contact", existing)
				doc.update(contact_data)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(contact_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Aspire Contact", "id": contact.get("ContactID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def sync_aspire_contracts(client, stats, modified_since=None, cutoff_date=None):
	"""Sync contracts from Aspire to Aspire Contract DocType.

	Filters applied in API:
	- OpportunityType = 'Contract'
	- OpportunityStatusName != 'Draft'
	- RenewalDate is set
	"""
	errors = []
	contracts = client.fetch_contracts(modified_since, cutoff_date)
	stats.pulled += len(contracts)

	# Build mappings
	company_map = {}
	aspire_companies = frappe.get_all("Aspire Company", fields=["name", "aspire_company_id"])
	for c in aspire_companies:
		if c.aspire_company_id:
			company_map[c.aspire_company_id] = c.name

	property_map = {}
	properties = frappe.get_all("Service Property", fields=["name", "aspire_property_id"])
	for p in properties:
		if p.aspire_property_id:
			property_map[p.aspire_property_id] = p.name

	for i, contract in enumerate(contracts):
		try:
			aspire_id = contract.get("OpportunityID")
			company_id = contract.get("BillingCompanyID") or contract.get("CompanyID")
			property_id = contract.get("PropertyID")

			company_name = company_map.get(company_id)
			property_name = property_map.get(property_id)

			# Skip if no company - Aspire Contract requires company
			if not company_name:
				continue

			contract_data = transform_to_aspire_contract(contract, company_name, property_name)

			# Check if exists by aspire_opportunity_id
			existing = frappe.db.get_value("Aspire Contract", {"aspire_opportunity_id": aspire_id}, "name")

			if existing:
				doc = frappe.get_doc("Aspire Contract", existing)
				for key, value in contract_data.items():
					if key not in ["doctype", "naming_series"]:
						setattr(doc, key, value)
				doc.save(ignore_permissions=True)
				stats.updated += 1
			else:
				doc = frappe.get_doc(contract_data)
				doc.insert(ignore_permissions=True)
				stats.created += 1

		except Exception as e:
			stats.errors += 1
			errors.append({"entity": "Aspire Contract", "id": contract.get("OpportunityID"), "error": str(e)})

		# Batch commit and delay
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			time.sleep(BATCH_DELAY)

	frappe.db.commit()
	return errors


def full_sync():
	"""Run a full sync of all entities from Aspire."""
	start_time = datetime.now()
	log_name = create_sync_log("Full", "All")
	stats = SyncStats()
	all_errors = []

	try:
		client = AspireClient()

		# Sync in order of dependencies
		frappe.publish_realtime("aspire_sync_progress", {"entity": "Companies", "status": "syncing"})
		all_errors.extend(sync_companies(client, stats))

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Properties", "status": "syncing"})
		all_errors.extend(sync_properties(client, stats))

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Contacts", "status": "syncing"})
		all_errors.extend(sync_contacts(client, stats))

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Opportunities", "status": "syncing"})
		all_errors.extend(sync_opportunities(client, stats))

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Work Tickets", "status": "syncing"})
		all_errors.extend(sync_work_tickets(client, stats))

		duration = (datetime.now() - start_time).total_seconds()
		status = "Success" if stats.errors == 0 else "Partial"
		update_sync_log(log_name, status, stats, all_errors, duration)

		frappe.publish_realtime("aspire_sync_progress", {"entity": "All", "status": "completed"})

	except AspireAPIError as e:
		duration = (datetime.now() - start_time).total_seconds()
		all_errors.append({"entity": "API", "error": str(e)})
		stats.errors += 1
		update_sync_log(log_name, "Failed", stats, all_errors, duration)

	return stats.to_dict()


def incremental_sync():
	"""Run an incremental sync since last successful sync."""
	start_time = datetime.now()
	log_name = create_sync_log("Incremental", "All")
	stats = SyncStats()
	all_errors = []

	modified_since = get_last_sync_date()

	try:
		client = AspireClient()

		all_errors.extend(sync_companies(client, stats, modified_since))
		all_errors.extend(sync_properties(client, stats, modified_since))
		all_errors.extend(sync_contacts(client, stats, modified_since))
		all_errors.extend(sync_opportunities(client, stats, modified_since))
		all_errors.extend(sync_work_tickets(client, stats, modified_since))

		duration = (datetime.now() - start_time).total_seconds()
		status = "Success" if stats.errors == 0 else "Partial"
		update_sync_log(log_name, status, stats, all_errors, duration)

	except AspireAPIError as e:
		duration = (datetime.now() - start_time).total_seconds()
		all_errors.append({"entity": "API", "error": str(e)})
		stats.errors += 1
		update_sync_log(log_name, "Failed", stats, all_errors, duration)

	return stats.to_dict()


@frappe.whitelist()
def manual_sync(entity_type="All"):
	"""Manually trigger a sync (callable from UI)."""
	frappe.enqueue(full_sync, queue="long", timeout=3600)
	return {"message": "Sync job queued"}


def sync_aspire_data(cutoff_date_str=None):
	"""
	Sync data to the new Aspire DocTypes (clean slate architecture).

	Sync order (respects foreign keys):
	1. Companies → Aspire Company
	2. Properties → Service Property (existing)
	3. Contacts → Aspire Contact
	4. Contracts → Aspire Contract (NOT draft, has renewal_date)

	Example: bench execute crystal_clean.integrations.aspire.sync.sync_aspire_data
	Example with cutoff: bench execute crystal_clean.integrations.aspire.sync.sync_aspire_data --kwargs '{"cutoff_date_str": "2025-01-01"}'
	"""
	cutoff = None
	if cutoff_date_str:
		cutoff = datetime.strptime(cutoff_date_str, "%Y-%m-%d")

	start_time = datetime.now()
	log_name = create_sync_log("Manual", "All")
	stats = SyncStats()
	all_errors = []

	try:
		client = AspireClient()

		# Sync Aspire Companies first (dependency for contacts and contracts)
		print("Syncing Companies → Aspire Company...")
		frappe.publish_realtime("aspire_sync_progress", {"entity": "Aspire Companies", "status": "syncing"})
		all_errors.extend(sync_aspire_companies(client, stats, cutoff_date=cutoff))
		print(f"  Companies: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		# Sync Properties (existing Service Property doctype)
		print("Syncing Properties → Service Property...")
		frappe.publish_realtime("aspire_sync_progress", {"entity": "Properties", "status": "syncing"})
		all_errors.extend(sync_properties(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		# Sync Aspire Contacts
		print("Syncing Contacts → Aspire Contact...")
		frappe.publish_realtime("aspire_sync_progress", {"entity": "Aspire Contacts", "status": "syncing"})
		all_errors.extend(sync_aspire_contacts(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		# Sync Aspire Contracts (NOT draft, has renewal_date)
		print("Syncing Contracts → Aspire Contract (filtered: NOT draft, has renewal_date)...")
		frappe.publish_realtime("aspire_sync_progress", {"entity": "Aspire Contracts", "status": "syncing"})
		all_errors.extend(sync_aspire_contracts(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		duration = (datetime.now() - start_time).total_seconds()
		status = "Success" if stats.errors == 0 else "Partial"
		update_sync_log(log_name, status, stats, all_errors, duration)

		frappe.publish_realtime("aspire_sync_progress", {"entity": "All", "status": "completed"})

		print(f"\n=== Aspire Data Sync Complete ===")
		print(f"Duration: {duration:.1f} seconds")
		print(f"Records pulled: {stats.pulled}")
		print(f"Created: {stats.created}")
		print(f"Updated: {stats.updated}")
		print(f"Errors: {stats.errors}")

	except AspireAPIError as e:
		duration = (datetime.now() - start_time).total_seconds()
		all_errors.append({"entity": "API", "error": str(e)})
		stats.errors += 1
		update_sync_log(log_name, "Failed", stats, all_errors, duration)
		print(f"FAILED: {e}")

	return stats.to_dict()


@frappe.whitelist()
def manual_aspire_sync():
	"""Manually trigger sync to new Aspire DocTypes (callable from UI)."""
	frappe.enqueue(sync_aspire_data, queue="long", timeout=3600)
	return {"message": "Aspire data sync job queued"}


def resync_since(cutoff_date_str="2025-10-01"):
	"""
	Resync all data modified since a specific date.

	Use this for catching up after failed syncs or initial setup.
	Example: bench execute crystal_clean.integrations.aspire.sync.resync_since --kwargs '{"cutoff_date_str": "2025-10-01"}'
	"""
	cutoff = datetime.strptime(cutoff_date_str, "%Y-%m-%d")

	start_time = datetime.now()
	log_name = create_sync_log("Manual", "All")
	stats = SyncStats()
	all_errors = []

	try:
		client = AspireClient()

		# Sync in order of dependencies with cutoff date
		frappe.publish_realtime("aspire_sync_progress", {"entity": "Companies", "status": "syncing"})
		print(f"Syncing Companies modified since {cutoff_date_str}...")
		all_errors.extend(sync_companies(client, stats, cutoff_date=cutoff))
		print(f"  Companies: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Properties", "status": "syncing"})
		print(f"Syncing Properties modified since {cutoff_date_str}...")
		all_errors.extend(sync_properties(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Contacts", "status": "syncing"})
		print(f"Syncing Contacts modified since {cutoff_date_str}...")
		all_errors.extend(sync_contacts(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Opportunities", "status": "syncing"})
		print(f"Syncing Opportunities modified since {cutoff_date_str}...")
		all_errors.extend(sync_opportunities(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		frappe.publish_realtime("aspire_sync_progress", {"entity": "Work Tickets", "status": "syncing"})
		print(f"Syncing Work Tickets modified since {cutoff_date_str}...")
		all_errors.extend(sync_work_tickets(client, stats, cutoff_date=cutoff))
		print(f"  Running totals: {stats.created} created, {stats.updated} updated, {stats.errors} errors")

		duration = (datetime.now() - start_time).total_seconds()
		status = "Success" if stats.errors == 0 else "Partial"
		update_sync_log(log_name, status, stats, all_errors, duration)

		frappe.publish_realtime("aspire_sync_progress", {"entity": "All", "status": "completed"})

		print(f"\n=== Resync Complete ===")
		print(f"Duration: {duration:.1f} seconds")
		print(f"Records pulled: {stats.pulled}")
		print(f"Created: {stats.created}")
		print(f"Updated: {stats.updated}")
		print(f"Errors: {stats.errors}")

	except AspireAPIError as e:
		duration = (datetime.now() - start_time).total_seconds()
		all_errors.append({"entity": "API", "error": str(e)})
		stats.errors += 1
		update_sync_log(log_name, "Failed", stats, all_errors, duration)
		print(f"FAILED: {e}")

	return stats.to_dict()


def link_work_tickets_to_properties():
	"""
	Link Work Tickets to Service Properties via OpportunityServices.

	Chain: WorkTicket.aspire_opportunity_service_id → OpportunityService.OpportunityID
	       → Opportunity.custom_aspire_opportunity_id → Opportunity.custom_service_property

	Example: bench execute crystal_clean.integrations.aspire.sync.link_work_tickets_to_properties
	"""
	print("Fetching OpportunityServices from Aspire API...")

	# Step 1: Fetch OpportunityServices from API
	client = AspireClient()
	opp_services = client.fetch_opportunity_services()
	print(f"  Fetched {len(opp_services)} OpportunityServices")

	# Step 2: Build OpportunityServiceID → OpportunityID map
	service_to_opp = {}
	for svc in opp_services:
		svc_id = svc.get("OpportunityServiceID")
		opp_id = svc.get("OpportunityID")
		if svc_id and opp_id:
			service_to_opp[svc_id] = opp_id

	print(f"  Built mapping for {len(service_to_opp)} services")

	# Step 3: Build OpportunityID → Service Property map from ERPNext
	opp_to_property = {}
	opportunities = frappe.get_all(
		"Opportunity",
		fields=["custom_aspire_opportunity_id", "custom_service_property"],
		filters={"custom_service_property": ["is", "set"]},
	)
	for opp in opportunities:
		if opp.custom_aspire_opportunity_id:
			opp_to_property[opp.custom_aspire_opportunity_id] = opp.custom_service_property

	print(f"  Found {len(opp_to_property)} Opportunities with Service Properties")

	# Step 4: Update Work Tickets
	work_tickets = frappe.get_all(
		"Work Ticket",
		fields=["name", "aspire_opportunity_service_id"],
		filters={
			"aspire_opportunity_service_id": ["is", "set"],
			"service_property": ["is", "not set"],
		},
	)

	print(f"  Found {len(work_tickets)} Work Tickets needing property link")

	updated = 0
	not_found = 0
	for i, wt in enumerate(work_tickets):
		svc_id = wt.aspire_opportunity_service_id
		opp_id = service_to_opp.get(svc_id)

		if opp_id:
			property_name = opp_to_property.get(opp_id)
			if property_name:
				frappe.db.set_value("Work Ticket", wt.name, "service_property", property_name, update_modified=False)
				updated += 1
			else:
				not_found += 1
		else:
			not_found += 1

		# Batch commit
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			print(f"  Progress: {i + 1}/{len(work_tickets)} processed, {updated} linked")

	frappe.db.commit()
	print(f"\n=== Link Complete ===")
	print(f"Work Tickets updated: {updated}")
	print(f"Could not link: {not_found}")

	return {"updated": updated, "not_found": not_found}


def link_properties_to_customers():
	"""
	Link Service Properties to Customers via Aspire CompanyID.

	Chain: ServiceProperty.aspire_property_id → Property.CompanyID
	       → Customer.custom_aspire_company_id

	Example: bench execute crystal_clean.integrations.aspire.sync.link_properties_to_customers
	"""
	print("Fetching Properties from Aspire API...")

	# Step 1: Fetch Properties from API to get PropertyID → CompanyID
	client = AspireClient()
	properties = client.fetch_properties()
	print(f"  Fetched {len(properties)} Properties from API")

	# Step 2: Build PropertyID → CompanyID map (CompanyID is in PropertyContacts array)
	property_to_company = {}
	for prop in properties:
		prop_id = prop.get("PropertyID")
		contacts = prop.get("PropertyContacts", [])
		# Get CompanyID from the first contact with a CompanyID
		company_id = None
		for contact in contacts:
			if contact.get("CompanyID"):
				company_id = contact.get("CompanyID")
				break
		if prop_id and company_id:
			property_to_company[prop_id] = company_id

	print(f"  Built mapping for {len(property_to_company)} properties")

	# Step 3: Build CompanyID → Customer name map from ERPNext
	company_to_customer = {}
	customers = frappe.get_all(
		"Customer",
		fields=["name", "custom_aspire_company_id"],
		filters={"custom_aspire_company_id": [">", 0]},
	)
	for cust in customers:
		company_to_customer[cust.custom_aspire_company_id] = cust.name

	print(f"  Found {len(company_to_customer)} Customers with Aspire IDs")

	# Step 4: Update Service Properties
	service_properties = frappe.get_all(
		"Service Property",
		fields=["name", "aspire_property_id"],
		filters={
			"aspire_property_id": [">", 0],
			"customer": ["is", "not set"],
		},
	)

	print(f"  Found {len(service_properties)} Service Properties needing customer link")

	updated = 0
	not_found = 0
	for i, sp in enumerate(service_properties):
		prop_id = sp.aspire_property_id
		company_id = property_to_company.get(prop_id)

		if company_id:
			customer_name = company_to_customer.get(company_id)
			if customer_name:
				frappe.db.set_value("Service Property", sp.name, "customer", customer_name, update_modified=False)
				updated += 1
			else:
				not_found += 1
		else:
			not_found += 1

		# Batch commit
		if (i + 1) % BATCH_SIZE == 0:
			frappe.db.commit()
			print(f"  Progress: {i + 1}/{len(service_properties)} processed, {updated} linked")

	frappe.db.commit()
	print(f"\n=== Link Complete ===")
	print(f"Service Properties updated: {updated}")
	print(f"Could not link: {not_found}")

	return {"updated": updated, "not_found": not_found}
