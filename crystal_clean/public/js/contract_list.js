frappe.listview_settings["Contract"] = {
	add_fields: ["status", "end_date", "party_name", "custom_sales_rep"],

	get_indicator: function(doc) {
		// Handle non-active statuses
		if (doc.status !== "Active") {
			if (doc.status === "Unsigned") {
				return [__("Unsigned"), "red", "status,=,Unsigned"];
			}
			return [__(doc.status), "gray", "status,=," + doc.status];
		}

		// Calculate days until expiry for Active contracts
		if (doc.end_date) {
			const days = frappe.datetime.get_diff(doc.end_date, frappe.datetime.get_today());

			if (days < 0) {
				return [__("Expired"), "red", "status,=,Active|end_date,<,Today"];
			} else if (days <= 30) {
				return [
					__("30 Days"),
					"orange",
					"status,=,Active|end_date,>=,Today|end_date,<=," + frappe.datetime.add_days(frappe.datetime.get_today(), 30)
				];
			} else if (days <= 60) {
				return [
					__("60 Days"),
					"yellow",
					"status,=,Active|end_date,>," + frappe.datetime.add_days(frappe.datetime.get_today(), 30) +
					"|end_date,<=," + frappe.datetime.add_days(frappe.datetime.get_today(), 60)
				];
			} else if (days <= 90) {
				return [
					__("90 Days"),
					"blue",
					"status,=,Active|end_date,>," + frappe.datetime.add_days(frappe.datetime.get_today(), 60) +
					"|end_date,<=," + frappe.datetime.add_days(frappe.datetime.get_today(), 90)
				];
			}
		}
		return [__("Active"), "green", "status,=,Active"];
	},

	onload: function(listview) {
		// Add "Create Issue" action for selected contracts
		listview.page.add_action_item(__("Create Issue"), function() {
			const selected = listview.get_checked_items();
			if (!selected.length) {
				frappe.msgprint(__("Please select at least one contract"));
				return;
			}

			// Create issue for the first selected contract
			frappe.call({
				method: "crystal_clean.crystal_clean.doctype.issue_process.issue_process.create_from_contract",
				args: { contract_name: selected[0].name },
				freeze: true,
				freeze_message: __("Creating Issue..."),
				callback: function(r) {
					if (r.message) {
						frappe.set_route("Form", "Issue Process", r.message);
					}
				}
			});
		});
	}
};
