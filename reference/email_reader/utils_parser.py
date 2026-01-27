# utils_parser.py
import re
from datetime import datetime

def parse_fixed_width_row(row, field_widths):
    result = {}
    idx = 0
    for field, width in field_widths.items():
        result[field] = row[idx: idx + width].strip()
        idx += width
    return result


def convert_types(field_map, parsed_row):
    converted = {}
    for header, db_field in field_map.items():
        raw = parsed_row.get(header)

        if db_field in ["customer_number", "vehicle_number", "enquiry_number"]:
            converted[db_field] = int(raw) if raw else None

        elif "date" in db_field:
            try:
                converted[db_field] = datetime.strptime(raw, "%d/%m/%Y").date()
            except:
                converted[db_field] = None

        else:
            converted[db_field] = raw

    return converted

