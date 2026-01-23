frappe.listview_settings["Issue Process"] = {
	add_fields: ["status", "priority", "due_date", "sales_rep", "expiry_bucket"],

	get_indicator: function(doc) {
		if (doc.status === "Completed") {
			return [__("Completed"), "green", "status,=,Completed"];
		} else if (doc.status === "Sent to Aspire") {
			return [__("Sent to Aspire"), "blue", "status,=,Sent to Aspire"];
		} else if (doc.status === "In Progress") {
			return [__("In Progress"), "orange", "status,=,In Progress"];
		} else if (doc.status === "Open") {
			if (doc.priority === "Critical") {
				return [__("Critical"), "red", "status,=,Open|priority,=,Critical"];
			} else if (doc.priority === "High") {
				return [__("High"), "orange", "status,=,Open|priority,=,High"];
			}
			return [__("Open"), "yellow", "status,=,Open"];
		}
		return [__(doc.status), "gray", "status,=," + doc.status];
	}
};
