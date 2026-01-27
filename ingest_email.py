import os
import msal
import requests
import logging
import base64
import glob
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Load environment variables
load_dotenv() # Try default local .env first

# Fallback: Check reference directory
ref_env_path = os.path.join("reference", "email_reader", ".env")
if os.path.exists(ref_env_path):
    log.info(f"Loading .env from {ref_env_path}")
    load_dotenv(ref_env_path)
    
# Debug: check if variables loaded
if not os.getenv("GRAPH_CLIENT_ID"):
    log.warning("GRAPH_CLIENT_ID not found in environment variables.")
else:
    log.info("Successfully loaded GRAPH_CLIENT_ID.")

# Configuration
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
IMAP_USER = os.getenv("IMAP_USER", "smf_ingestion@tequila-ai.com")
TARGET_SENDER = "noreply@3cx.net"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
DATA_DIR = "data"

def get_graph_token():
    """Acquire MS Graph API token."""
    if not all([GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET]):
        log.error("Missing required environment variables (GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET)")
        return None

    app = msal.ConfidentialClientApplication(
        client_id=GRAPH_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}",
        client_credential=GRAPH_CLIENT_SECRET,
    )
    token = app.acquire_token_for_client(scopes=[GRAPH_SCOPE])
    if "access_token" not in token:
        log.error(f"Graph Auth Failed: {token.get('error_description')}")
        return None
    return token["access_token"]

def fetch_unread_emails(token):
    """Fetch unread emails from the target sender."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Filter: Unread AND from target sender
    # Expand: Attachments
    url = (
        f"https://graph.microsoft.com/v1.0/users/{IMAP_USER}/messages"
        f"?$filter=isRead eq false and from/emailAddress/address eq '{TARGET_SENDER}'"
        f"&$expand=attachments"
        f"&$select=id,subject,receivedDateTime,hasAttachments"
    )
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching emails: {e}")
        return []

def mark_as_read(token, message_id):
    """Mark a specific email as read."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"https://graph.microsoft.com/v1.0/users/{IMAP_USER}/messages/{message_id}"
    data = {"isRead": True}
    
    try:
        requests.patch(url, headers=headers, json=data).raise_for_status()
        log.info(f"Marked message {message_id} as read.")
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to mark message {message_id} as read: {e}")

def process_attachments(message, token):
    """Download and process attachments from the email."""
    attachments = message.get("attachments", [])
    processed_count = 0
    
    for att in attachments:
        name = att.get("name", "")
        # Check patterns
        is_call_log = "CallLogLastWeek" in name and name.endswith(".xlsx")
        is_abandoned = "AbandonedCallslastweek" in name and name.endswith(".xlsx") # Note spelling 'Abanodoned' in user prompt, but likely standard 'Abandoned' or user specific. checking both
        is_abandoned_typo = "AbanodonedCallslastweek" in name and name.endswith(".xlsx")

        if is_call_log or is_abandoned or is_abandoned_typo:
            log.info(f"Found match: {name}")
            
            # Get content
            content_b64 = att.get("contentBytes")
            if not content_b64:
                continue
                
            content = base64.b64decode(content_b64)
            
            # Save original XLSX
            xlsx_path = os.path.join(DATA_DIR, name)
            with open(xlsx_path, "wb") as f:
                f.write(content)
            log.info(f"Saved: {xlsx_path}")
            
            # Convert to CSV for compatibility
            try:
                # Generate CSV filename (replace .xlsx with .csv)
                csv_name = name.rsplit('.', 1)[0] + ".csv"
                # If renaming/cleaning needed for standard pipeline:
                # User's current files: CallLogLastWeek_DDMM_....csv
                # We'll just keep the base name and change extension.
                
                csv_path = os.path.join(DATA_DIR, csv_name)
                
                log.info(f"Converting {name} to CSV...")
                df = pd.read_excel(xlsx_path)
                df.to_csv(csv_path, index=False)
                log.info(f"Converted and saved: {csv_path}")
                processed_count += 1
                
                # Optional: Remove xlsx to keep data dir clean? 
                # User said "download the data into the data folder".
                # Keeping both is safer for now, existing pipeline ignores .xlsx
                
            except Exception as e:
                log.error(f"Error converting {name} to CSV: {e}")

    return processed_count

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    token = get_graph_token()
    if not token:
        print("Failed to authenticate. Check .env credentials.")
        return

    print(f"Checking for unread emails from {TARGET_SENDER}...")
    emails = fetch_unread_emails(token)
    
    if not emails:
        print("No unread emails found.")
        return
        
    print(f"Found {len(emails)} unread emails.")
    
    for email in emails:
        subject = email.get("subject", "No Subject")
        msg_id = email.get("id")
        log.info(f"Processing: {subject}")
        
        # Process attachments
        count = process_attachments(email, token)
        if count > 0:
            log.info(f" -> Processed {count} files from email.")
        else:
            log.info(" -> No matching attachments found.")
            
        # Mark as read (User requirement: "mark all the emails as read")
        mark_as_read(token, msg_id)

if __name__ == "__main__":
    main()
