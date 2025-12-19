# Copyright (c) 2024, Crystal Clean Maintenance and contributors
# For license information, please see license.txt

"""
Aspire Cloud API Client

Auth: POST /Authorization with {"ClientID": "...", "Secret": "..."}
Response: {"Token": "...", "RefreshToken": "..."}

Data endpoints return raw JSON arrays, NOT OData format.
Pagination via $top and $skip parameters.
$filter is supported for filtering.
$select is NOT supported (causes 400 error).
"""

import time
from datetime import datetime, timedelta

import frappe
import requests


class AspireAPIError(Exception):
	"""Exception raised for Aspire API errors."""

	def __init__(self, message, status_code=None, response=None):
		self.message = message
		self.status_code = status_code
		self.response = response
		super().__init__(self.message)


class AspireClient:
	"""Client for interacting with Aspire Cloud API."""

	def __init__(self):
		self.base_url = frappe.conf.get("aspire_api_base_url")
		self.client_id = frappe.conf.get("aspire_api_client_id")
		self.api_key = frappe.conf.get("aspire_api_key")

		if not all([self.base_url, self.client_id, self.api_key]):
			raise AspireAPIError(
				"Aspire API credentials not configured. "
				"Set aspire_api_base_url, aspire_api_client_id, and aspire_api_key in site_config.json"
			)

		self._token = None
		self._token_expires = None
		self.page_size = 100

	def _get_auth_token(self):
		"""Get OAuth bearer token from Aspire API."""
		if self._token and self._token_expires and datetime.now() < self._token_expires:
			return self._token

		auth_url = f"{self.base_url}/Authorization"
		payload = {"ClientID": self.client_id, "Secret": self.api_key}

		try:
			response = requests.post(auth_url, json=payload, timeout=30)
			response.raise_for_status()
			data = response.json()

			self._token = data.get("Token")
			# Token expires in ~60 mins, refresh at 50 mins
			self._token_expires = datetime.now() + timedelta(minutes=50)

			return self._token
		except requests.exceptions.RequestException as e:
			raise AspireAPIError(f"Failed to authenticate with Aspire API: {e}")

	def _make_request(self, endpoint, params=None, method="GET"):
		"""Make an authenticated request to the Aspire API."""
		token = self._get_auth_token()
		url = f"{self.base_url}/{endpoint}"

		headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

		try:
			response = requests.request(method, url, headers=headers, params=params, timeout=120)
			response.raise_for_status()
			return response.json()
		except requests.exceptions.HTTPError as e:
			raise AspireAPIError(
				f"Aspire API request failed: {e}", status_code=response.status_code, response=response.text
			)
		except requests.exceptions.RequestException as e:
			raise AspireAPIError(f"Failed to connect to Aspire API: {e}")

	def _fetch_all_pages(self, endpoint, params=None):
		"""Fetch all pages of results using $top/$skip pagination."""
		if params is None:
			params = {}

		all_records = []
		skip = 0

		while True:
			page_params = {**params, "$top": self.page_size, "$skip": skip}
			data = self._make_request(endpoint, page_params)

			# API returns raw array
			if isinstance(data, list):
				if not data:
					break
				all_records.extend(data)
				if len(data) < self.page_size:
					break
				skip += self.page_size
			else:
				# Unexpected format
				frappe.log_error(f"Unexpected API response format: {type(data)}", "Aspire API")
				break

			# Rate limiting - be nice to the API
			time.sleep(0.1)

		return all_records

	def fetch_companies(self, modified_since=None, cutoff_date=None):
		"""Fetch companies from Aspire API."""
		params = {}
		filters = []
		if cutoff_date:
			filters.append(f"ModifiedDateTime ge {cutoff_date.strftime('%Y-%m-%d')}T00:00:00Z")
		if modified_since:
			filters.append(f"ModifiedDateTime gt {modified_since.isoformat()}Z")
		if filters:
			params["$filter"] = " and ".join(filters)
		return self._fetch_all_pages("Companies", params)

	def fetch_contacts(self, modified_since=None, cutoff_date=None):
		"""Fetch contacts from Aspire API."""
		filters = ["Email ne null and Active eq true"]
		if cutoff_date:
			filters.append(f"ModifiedDate ge {cutoff_date.strftime('%Y-%m-%d')}T00:00:00Z")
		if modified_since:
			filters.append(f"ModifiedDate gt {modified_since.isoformat()}Z")
		params = {"$filter": " and ".join(filters)}
		return self._fetch_all_pages("Contacts", params)

	def fetch_properties(self, modified_since=None, cutoff_date=None):
		"""Fetch active properties from Aspire API."""
		filters = ["Active eq true"]
		if cutoff_date:
			filters.append(f"ModifiedDate ge {cutoff_date.strftime('%Y-%m-%d')}T00:00:00Z")
		if modified_since:
			filters.append(f"ModifiedDate gt {modified_since.isoformat()}Z")
		params = {"$filter": " and ".join(filters)}
		return self._fetch_all_pages("Properties", params)

	def fetch_opportunities(self, modified_since=None, cutoff_date=None):
		"""Fetch won contract opportunities from Aspire API."""
		filters = ["OpportunityType eq 'Contract' and OpportunityStatusName eq '7. Won'"]
		if cutoff_date:
			filters.append(f"ModifiedDate ge {cutoff_date.strftime('%Y-%m-%d')}T00:00:00Z")
		if modified_since:
			filters.append(f"ModifiedDate gt {modified_since.isoformat()}Z")
		params = {"$filter": " and ".join(filters)}
		return self._fetch_all_pages("Opportunities", params)

	def fetch_contracts(self, modified_since=None, cutoff_date=None):
		"""Fetch all contract opportunities (NOT draft, has renewal date) from Aspire API.

		Filters:
		- OpportunityType = 'Contract'
		- OpportunityStatusName != 'Draft' (ne '1. Draft')
		- RenewalDate is set (ne null)
		"""
		# All contracts that are not draft and have a renewal date
		filters = [
			"OpportunityType eq 'Contract'",
			"OpportunityStatusName ne '1. Draft'",
			"RenewalDate ne null",
		]
		if cutoff_date:
			filters.append(f"ModifiedDate ge {cutoff_date.strftime('%Y-%m-%d')}T00:00:00Z")
		if modified_since:
			filters.append(f"ModifiedDate gt {modified_since.isoformat()}Z")
		params = {"$filter": " and ".join(filters)}
		return self._fetch_all_pages("Opportunities", params)

	def fetch_opportunity_services(self, opportunity_ids=None):
		"""Fetch opportunity services from Aspire API."""
		params = {}
		if opportunity_ids:
			ids_str = ",".join(str(id) for id in opportunity_ids)
			params["$filter"] = f"OpportunityID in ({ids_str})"
		return self._fetch_all_pages("OpportunityServices", params)

	def fetch_work_tickets(self, modified_since=None, cutoff_date=None, months_back=6):
		"""Fetch work tickets from Aspire API (last N months or since cutoff_date)."""
		if cutoff_date:
			# If cutoff_date provided, use it for ScheduledStartDate filter
			schedule_cutoff = cutoff_date
		else:
			# Otherwise use months_back from now
			schedule_cutoff = datetime.now() - timedelta(days=months_back * 30)

		# WorkTickets uses LastModifiedDateTime (not ModifiedDate like other endpoints)
		filters = [f"ScheduledStartDate ge {schedule_cutoff.strftime('%Y-%m-%d')}"]
		if cutoff_date:
			filters.append(f"LastModifiedDateTime ge {cutoff_date.strftime('%Y-%m-%d')}T00:00:00Z")
		if modified_since:
			filters.append(f"LastModifiedDateTime gt {modified_since.isoformat()}Z")
		params = {"$filter": " and ".join(filters)}
		return self._fetch_all_pages("WorkTickets", params)

	def test_connection(self):
		"""Test connection to Aspire API."""
		try:
			self._get_auth_token()
			return True
		except AspireAPIError:
			return False
