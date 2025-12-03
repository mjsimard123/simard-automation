import imaplib
import email
from email.header import decode_header
import firebase_admin
from firebase_admin import credentials, firestore
from bs4 import BeautifulSoup
import os
import hashlib
import datetime

# --- CONFIGURATION ---
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
IMAP_SERVER = "imap.gmail.com"
SEARCH_SUBJECT = 'Appt InSights'
CRED_PATH = 'serviceAccountKey.json' 
APP_ID = "simard-insights-app" 

# Initialize Firebase
if not firebase_admin._apps:
    cred = credentials.Certificate(CRED_PATH)
    firebase_admin.initialize_app(cred)

db = firestore.client()

def connect_to_mail():
    try:
        print("Attempting to connect to Gmail...")
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_USER, EMAIL_PASS)
        print("✅ Login Successful")
        return mail
    except Exception as e:
        print(f"❌ Error connecting to email: {e}")
        return None

def extract_call_data(html_content):
    """Parses the HTML table from the email to find call rows."""
    soup = BeautifulSoup(html_content, "html.parser")
    rows_data = []
    
    # Look for table rows
    rows = soup.find_all('tr')
    print(f"   - Found {len(rows)} rows in email table.")
    
    for row in rows:
        cols = row.find_all(['td', 'th'])
        cols = [ele.text.strip() for ele in cols]
        
        # Valid call rows usually have at least 5 columns
        if len(cols) >= 5: 
            call_record = {
                "date": cols[0],      # e.g., 2025-10-25
                "time": cols[1],      # e.g., 10:30 AM
                "caller": cols[2],    # e.g., (555) 123-4567
                "agent": cols[3],     # e.g., Sarah
                "store": cols[4],     # e.g., Simard North
                "status": cols[5] if len(cols) > 5 else "Unknown",
                "duration": cols[6] if len(cols) > 6 else "",
                "notes": cols[7] if len(cols) > 7 else ""
            }
            # Only add if it looks like a real data row (has a year)
            if "202" in call_record['date']:
                rows_data.append(call_record)
            
    return rows_data

def push_to_firestore(data):
    if not data:
        print("⚠️ No call data extracted from this email.")
        return
    
    collection_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('calls')
    
    count = 0
    for record in data:
        # Create a unique ID to prevent duplicates
        unique_string = f"{record['date']}{record['time']}{record['caller']}"
        doc_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        doc_ref = collection_ref.document(doc_id)
        doc_ref.set(record, merge=True)
        count += 1
            
    print(f"✅ Successfully synced {count} records to Firestore.")

def process_email():
    mail = connect_to_mail()
    if not mail:
        return

    mail.select("inbox")
    
    print(f"--- SEARCHING FOR: {SEARCH_SUBJECT} ---")
    status, messages = mail.search(None, f'(SUBJECT "{SEARCH_SUBJECT}")')
    
    if status == "OK":
        email_ids = messages[0].split()
        if not email_ids:
            print(f"❌ No emails found with subject '{SEARCH_SUBJECT}'")
            return

        # Process the LATEST email found
        latest_id = email_ids[-1]
        print(f"✅ Found {len(email_ids)} emails. Processing the latest one...")
        
        _, msg_data = mail.fetch(latest_id, "(RFC822)")
        for response in msg_data:
            if isinstance(response, tuple):
                msg = email.message_from_bytes(response[1])
                
                # Get HTML Body
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            body = part.get_payload(decode=True).decode()
                            break
                else:
                    if msg.get_content_type() == "text/html":
                        body = msg.get_payload(decode=True).decode()

                if body:
                    data = extract_call_data(body)
                    push_to_firestore(data)
                else:
                    print("⚠️ Email had no HTML content.")
                            
    mail.logout()

if __name__ == "__main__":
    process_email()
