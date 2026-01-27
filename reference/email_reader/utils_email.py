# utils_email.py
import base64
import requests
import os
import msal
import requests
import logging
import dotenv
import base64
from dotenv import load_dotenv
import json
from typing import Dict, Any

log = logging.getLogger(__name__)

load_dotenv()

# Load env vars

IMAP_USER = os.getenv("IMAP_USER")
RECIPIENT = os.getenv("RECIPIENT").lower()
ALLOWED_DOMAINS = {d.strip().lower() for d in os.getenv("ALLOWED_DOMAINS", "").split(",")}
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

def get_graph_token():
    app = msal.ConfidentialClientApplication(
        client_id=GRAPH_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}",
        client_credential=GRAPH_CLIENT_SECRET,
    )
    token = app.acquire_token_for_client(scopes=[GRAPH_SCOPE])
    if "access_token" not in token:
        raise RuntimeError(f"Graph Auth Failed: {token}")
    return token["access_token"]


def reply_with_attachment(token, message_id, excel_bytes, filename="processed.xlsx"):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    reply_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}/reply"

    # Create draft reply
    draft = requests.post(reply_url, headers=headers)
    draft.raise_for_status()
    draft_id = draft.json()["id"]

    # Attach file
    attach_url = f"https://graph.microsoft.com/v1.0/me/messages/{draft_id}/attachments"

    attachment = {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": filename,
        "contentBytes": excel_bytes.getvalue().hex(),
        "size": len(excel_bytes)
    }

    r = requests.post(attach_url, headers=headers, json=attachment)
    r.raise_for_status()

    # Send it
    send_url = f"https://graph.microsoft.com/v1.0/me/messages/{draft_id}/send"
    final = requests.post(send_url, headers=headers)
    final.raise_for_status()

# Assume get_graph_token() and IMAP_USER are defined elsewhere

def send_new_email(subject: str, body_content: str, recipient_email: str) -> None:
    """
    Sends a new email message using the Microsoft Graph sendMail endpoint.
    """
    token = get_graph_token()

    # 1. Use the sendMail endpoint
    url = f"https://graph.microsoft.com/v1.0/users/{IMAP_USER}/sendMail"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 3. Create the JSON payload for the new message
    email_body: Dict[str, Any] = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text", # Use "Html" for rich text
                "content": body_content
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient_email
                    }
                }
            ]
            # Optionally add "ccRecipients" or "attachments" here
        },
        "saveToSentItems": "true"  # Recommended: saves a copy to the Sent Items folder
    }

    # 2. Send the POST request
    try:
        res = requests.post(url, headers=headers, data=json.dumps(email_body))
        res.raise_for_status()
        log.info(f"Successfully sent email to {recipient_email}")
        
    except requests.exceptions.HTTPError as e:
        log.info(f"Error sending email: {e}")
        log.info(f"Response: {res.text}")
        
    except requests.exceptions.RequestException as e:
        log.info(f"A request error occurred: {e}")


# Assume get_graph_token() and IMAP_USER are defined elsewhere

def send_email_with_attachment(
    subject: str, 
    body_content: str,
    excel_content: str,
    file_name: str,
    recipient_email: str
) -> None:
    """
    Sends a new email message with a file attached using the Microsoft Graph sendMail endpoint.
    
    Args:
        subject (str): The subject line of the email.
        body_content (str): The text content of the email body.
        recipient_email (str): The email address of the primary recipient.
        file_path (str): The local path to the file to attach (e.g., 'report.xlsx').
    """
    excel_base64 = base64.b64encode(excel_content).decode() 

    attachment_payload = {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": file_name,
        "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", # MIME type for .xlsx
        "contentBytes": excel_base64
    }

    # --- 3. Build the Full Email Payload ---
    token = get_graph_token()
    url = f"https://graph.microsoft.com/v1.0/users/{IMAP_USER}/sendMail"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    email_body: Dict[str, Any] = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body_content
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient_email
                    }
                }
            ],
            # Add the attachment list here
            "attachments": [attachment_payload]
        },
        "saveToSentItems": "true"
    }

    # --- 4. Send the Request ---
    try:
        res = requests.post(url, headers=headers, data=json.dumps(email_body))
        res.raise_for_status()
        log.info(f"Successfully sent email with attachment '{file_name}' to {recipient_email}")
        return True 
    except requests.exceptions.HTTPError as e:
        log.info(f"Error sending email: {e}")
        log.info(f"Response: {res.text}")
        return False
        
    except requests.exceptions.RequestException as e:
        log.info(f"A request error occurred: {e}")
        return False

def mark_email_as_read(message_id):
    token = get_graph_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"https://graph.microsoft.com/v1.0/users/{os.getenv('IMAP_USER')}/messages/{message_id}"
    data = {"isRead": True}

    res = requests.patch(url, headers=headers, json=data)
    res.raise_for_status()


