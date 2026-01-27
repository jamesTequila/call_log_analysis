# utils_excel.py
import pandas as pd
from io import BytesIO

def make_excel(rows):
    df = pd.DataFrame(rows)
    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)
    return buffer

def create_excel_from_records(records):
    if not records:
        return None
    
    df = pd.DataFrame(records)
    
    # Optional: reorder columns or format as needed
    # df = df[['customer_number', 'vehicle_number', 'make', 'model', ...]]
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='CustomerCarData', index=False)
    
    output.seek(0)
    return output.read()
