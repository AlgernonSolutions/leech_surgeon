def _decompose_dict(provided_dict):
    results = []
    for key, value in provided_dict.items():
        if isinstance(value, dict):
            value = _decompose_dict(value)
        if isinstance(value, list):
            results.extend(value)
            continue
        results.append(value)
    return results


class ReportData:
    def __init__(self, data_name, report_entries):
        self._data_name = data_name
        self._report_entries = report_entries

    @classmethod
    def from_stored_data(cls, data_name, stored_data):
        if isinstance(stored_data, dict):
            stored_data = _decompose_dict(stored_data)
        return cls(data_name, stored_data)

    @property
    def data_name(self):
        return self._data_name

    def line_item_filter(self, filter_fn, filter_args):
        line_items = []
        for entry in self._report_entries:
            if filter_fn(entry, *filter_args):
                line_items.append(entry)
        return ReportData(self._data_name, line_items)

    def for_spreadsheet(self, printed_header_row, header_row):
        sheet = [printed_header_row]
        for entry in self._report_entries:
            sheet_row = []
            for header in header_row:
                sheet_row.append(entry[header])
            sheet.append(sheet_row)
        return sheet
