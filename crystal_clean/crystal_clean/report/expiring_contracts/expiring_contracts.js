// Copyright (c) 2025, Crystal Clean and contributors
// For license information, please see license.txt

frappe.query_reports["Expiring Contracts"] = {
	filters: [
		{
			fieldname: "sales_rep",
			label: __("Sales Rep"),
			fieldtype: "Data",
		},
		{
			fieldname: "from_date",
			label: __("From Date"),
			fieldtype: "Date",
			default: frappe.datetime.get_today(),
		},
		{
			fieldname: "to_date",
			label: __("To Date"),
			fieldtype: "Date",
		},
		{
			fieldname: "days",
			label: __("Expiring Within (Days)"),
			fieldtype: "Select",
			options: [
				{ value: "", label: __("All") },
				{ value: "30", label: __("30 Days") },
				{ value: "60", label: __("60 Days") },
				{ value: "90", label: __("90 Days") },
			],
		},
	],

	formatter(value, row, column, data, default_formatter) {
		if (column.fieldname === "action" && data) {
			return `<button class="btn btn-xs btn-primary"
				onclick="frappe.query_reports['Expiring Contracts'].create_issue('${data.name}')">
				Create Issue
			</button>`;
		}
		return default_formatter(value, row, column, data);
	},

	create_issue(contract_name) {
		frappe.call({
			method: "crystal_clean.crystal_clean.api.create_issue_from_contract",
			args: { contract_name: contract_name },
			callback: (r) => {
				if (r.message) {
					frappe.show_alert({
						message: __("Issue Process {0} created", [r.message]),
						indicator: "green"
					});
					frappe.set_route("Form", "Issue Process", r.message);
				}
			}
		});
	}
};
