import csv
import datetime
import io
import re
from decimal import Decimal

import pytz


class CredibleCsvParser:
    _field_value_maps = {
        'Date': 'datetime',
        'Service Date': 'date',
        'DOB': 'utc_datetime',
        'Time In': 'datetime',
        'Time Out': 'datetime',
        'Service ID': 'number',
        'UTCDate': 'utc_datetime',
        'change_date': 'datetime',
        'by_emp_id': 'number',
        'Transfer Date': 'datetime',
        'Approved Date': 'datetime'
    }

    def __init__(self, id_source, id_source_tz: pytz.timezone):
        self._id_source = id_source
        self._id_source_tz = id_source_tz

    def parse_csv_response(self, csv_string, key_name=None):
        response = []
        if key_name:
            response = {}
        header = []
        first = True
        escaped = re.sub(r'(?<!^)(?<!,)"(?!,|$)', "'", csv_string)
        with io.StringIO(escaped, newline='\r\n') as io_string:
            reader = csv.reader(io_string)
            for row in reader:
                header_index = 0
                row_entry = {}
                if first:
                    for entry in row:
                        if entry not in header:
                            header.append(entry)
                    first = False
                    continue
                for entry in row:
                    try:
                        header_name = header[header_index]
                    except IndexError:
                        raise RuntimeError(
                            'the returned data from a csv query contained insufficient information to create the table')
                    entry = self._set_data_type(header_name, entry)
                    row_entry[header_name] = entry
                    header_index += 1
                if key_name:
                    key_value = row_entry[key_name]
                    response[key_value] = row_entry
                    continue
                response.append(row_entry)
        return response

    def _set_data_type(self, header_name, entry):
        data_type = self._field_value_maps.get(header_name, 'string')
        if not entry:
            return None
        if data_type == 'string':
            entry = str(entry)
        if data_type == 'datetime':
            try:
                naive_entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            except ValueError:
                entry = f'{entry} 12:00:00 AM'
                naive_entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            local_tz_entry = self._id_source_tz.localize(naive_entry)
            entry = local_tz_entry.astimezone(pytz.UTC)
        if data_type == 'date':
            naive_entry = datetime.datetime.strptime(entry, '%m/%d/%Y')
            entry = pytz.UTC.localize(naive_entry)
        if data_type == 'utc_datetime':
            try:
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            except ValueError:
                entry = f'{entry} 12:00:00 AM'
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            entry = entry.replace(tzinfo=pytz.UTC)
        if data_type == 'number':
            entry = Decimal(entry)
        return entry
