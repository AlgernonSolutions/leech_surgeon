import csv
import datetime
import io
import re
from decimal import Decimal

import pytz


def _fix_unescaped_quotes(csv_string):
    escaped = re.sub(r'(,) +', '\\, ', csv_string)
    return escaped



class CredibleCsvParser:
    _field_value_maps = {
        'Date': 'datetime',
        'Service Date': 'date',
        'Time In': 'datetime',
        'Time Out': 'datetime',
        'Service ID': 'number',
        'UTCDate': 'utc_datetime',
        'change_date': 'datetime',
        'by_emp_id': 'number',
        'Transfer Date': 'datetime',
        'Approved Date': 'datetime'
    }

    @classmethod
    def parse_csv_response(cls, csv_string, key_name=None):
        response = []
        if key_name:
            response = {}
        header = []
        first = True
        with io.StringIO(csv_string, newline='\r\n') as io_string:
            reader = csv.reader(io_string, escapechar='\\')
            lines = csv_string.split('\r\n')
            for row_number, row in enumerate(reader):
                if first:
                    for entry in row:
                        header.append(entry)
                    first = False
                    continue
                row_entry = cls._parse_row(header, row_number, row, lines)
                if key_name:
                    key_value = row_entry[key_name]
                    response[key_value] = row_entry
                    continue
                response.append(row_entry)
        return response

    @classmethod
    def _parse_row(cls, header, row_number, row, lines):
        header_index = 0
        row_entry = {}
        if len(row) > len(header):
            problem_line = lines[row_number]
            return cls._resolve_unescaped_line(header, problem_line)
        for entry in row:
            try:
                header_name = header[header_index]
            except IndexError:
                raise RuntimeError(
                    'the returned data from a csv query contained insufficient information to create the table')
            entry = cls._set_data_type(header_name, entry)
            row_entry[header_name] = entry
            header_index += 1
        if len(row_entry) != len(header):
            print()
        return row_entry

    @classmethod
    def _resolve_unescaped_line(cls, header, problem_line):
        row_entry = {}
        header_index = 0
        escaped = _fix_unescaped_quotes(problem_line)
        with io.StringIO(escaped, newline='\r\n') as io_string:
            reader = csv.reader(io_string, escapechar='\\')
            for row in reader:
                for entry in row:
                    try:
                        header_name = header[header_index]
                    except IndexError:
                        raise RuntimeError(
                            'the returned data from a csv query contained insufficient information to create the table')
                    entry = cls._set_data_type(header_name, entry)
                    row_entry[header_name] = entry
                    header_index += 1
            return row_entry

    @classmethod
    def _set_data_type(cls, header_name, entry):
        data_type = cls._field_value_maps.get(header_name, 'string')
        if not entry:
            return None
        if data_type == 'string':
            entry = str(entry)
        if data_type == 'datetime':
            try:
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            except ValueError:
                entry = f'{entry} 12:00:00 AM'
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
        if data_type == 'date':
            entry = datetime.datetime.strptime(entry, '%m/%d/%Y')
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
