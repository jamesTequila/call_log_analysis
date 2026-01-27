# process_email_graph.py
import os
import logging
import json
import dotenv
from db import SessionLocal
from dotenv import load_dotenv
from models import CustomerCarData, IngestionAudit
from utils_parser import parse_fixed_width_row, convert_types
from utils_excel import make_excel,create_excel_from_records
from utils_email import mark_email_as_read
#from utils_email import reply_with_attachment, send_email_with_attachment
from utils_data import sanitize_filename
from mailprocessor import fetch_messages, filter_messages, send_email_with_attachment
from pprint import pprint
from sqlalchemy.dialects import postgresql
from sqlalchemy import insert 

load_dotenv()

ORIGINATOR = os.getenv("ORIGINATOR").lower()

logging.basicConfig(
    filename="logs/mail_processor.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

FIELD_MAP = {
    "Customer Number": "customer_number",
    "Customer Full Name": "customer_full_name",
    "Telephone Number": "telephone_number",
    "E-mail Address": "email_address",
    "Enquiry Creation Date": "enquiry_creation_date",
    "Enquiry Number": "enquiry_number",
    "Deposit Date": "deposit_date",
    "Invoice Date": "invoice_date",
    "Vehicle Number": "vehicle_number",
    "Model": "model",
    "Variant": "variant",
    "Colour Code": "colour_code",
    "Registration Number": "registration_number",
}

# Define fixed field widths (sum must match data row length)
FIELD_WIDTHS = {
    "Customer Number": 17,
    "Customer Full Name": 136,
    "Telephone Number": 31,
    "E-mail Address": 101,
    "Enquiry Creation Date": 31,
    "Enquiry Number": 6,
    "Deposit Date": 13,
    "Invoice Date": 22,
    "Vehicle Number": 6,
    "Model": 16,
    "Variant": 16,
    "Colour Code": 16,
    "Registration Number": 9,
}

def pdict(d):
    print("\n=== DICT DEBUG ===")
    pprint(d, width=50)
    print("\nJSON version:")
    print(json.dumps(d, indent=2, default=str))
    log.info(json.dumps(d, indent=2, default=str))

def process_emails():

    messages, token = fetch_messages()
    if not messages:
        log.info("No unread messages")
        print("No unread messages")
        return

    allowed_messages = filter_messages(messages)

    session = SessionLocal()
    first = True
    for msg in allowed_messages:
        subject = msg.get("subject", "")
        filename = sanitize_filename(subject)
        body_content = "data from keyloop"
        email_id = msg["id"]
        #raw_body = msg.get("body", "")
        raw_body = msg.get("body", {}).get("content", "")
        #if  first:
        #    pdict(msg)
        #    first = False
        #print(f"Raw body type: {type(raw_body)}")
        #print(f"Raw body (first 200 chars): {raw_body[:200]}")
        lines = raw_body.splitlines()
        if len(lines) < 2:
            log.warning(f"Email {email_id} has no data rows")
            continue
        print(lines[2])
        data_rows = lines[1:]  # skip header
        parsed_records = []
        inserted = 0

        for row in data_rows:
            if len(row) == 0:
                continue
               
            parsed = parse_fixed_width_row(row, FIELD_WIDTHS)
            #pdict(parsed)
            converted = convert_types(FIELD_MAP, parsed)
            parsed_records.append(converted)
                #pdict(converted)

            # UPSERT logic
            stmt = (
                postgresql.insert(CustomerCarData.__table__)
                #    .values(**converted)
                #CustomerCarData.__table__.insert()
                .values(**converted)
                .on_conflict_do_update(
                    index_elements=["customer_number", "vehicle_number"],
                    set_=converted
                )
            )
            session.execute(stmt)
            inserted += 1

        # Write audit
        audit = IngestionAudit(
            email_id=email_id,
            raw_body=raw_body,
            rows_extracted=len(parsed_records),
            rows_inserted=inserted,
        )
        session.add(audit)
        session.commit()

        # Build Excel and reply
        # excel_data = make_excel(parsed_records)
        excel_data = create_excel_from_records(parsed_records)

        send_email_with_attachment( subject, body_content, excel_data, filename, ORIGINATOR)

        mark_email_as_read(email_id)


        #reply_with_attachment(token, email_id, excel_bytes)

        log.info(f"Processed message {email_id}: {inserted} rows inserted")

    session.close()


if __name__ == "__main__":
    process_emails()

