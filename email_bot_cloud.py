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

def push_test_record():
    print("--- PUSHING TEST RECORD ---")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    test_data = {
        "date": today,
        "time": "12:00 PM",
        "caller": "TEST ENTRY (Debug)",
        "agent": "System Admin",
        "store": "Simard Debug",
        "status": "Appt Booked",
        "duration": "1m 0s",
        "notes": "If you see this, the Database connection works!"
    }
    
    try:
        doc_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('calls').document('debug_test')
        doc_ref.set(test_data)
        print("✅ Test Record sent to Firestore!")
    except Exception as e:
        print(f"❌ Failed to write to Firestore: {e}")

def process_email():
    mail = connect_to_mail()
    if not mail:
        return

    mail.select("inbox")
    
    # DEBUG: List last 3 emails to check subjects
    print("--- CHECKING LAST 3 EMAILS IN INBOX ---")
    _, messages = mail.search(None, 'ALL')
    email_ids = messages[0].split()[-3:]
    
    for e_id in email_ids:
        _, msg_data = mail.fetch(e_id, "(RFC822)")
        for response in msg_data:
            if isinstance(response, tuple):
                msg = email.message_from_bytes(response[1])
                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding if encoding else "utf-8")
                print(f"Found Email: {subject}")

    # REAL SEARCH
    print(f"--- SEARCHING FOR: {SEARCH_SUBJECT} ---")
    status, messages = mail.search(None, f'(SUBJECT "{SEARCH_SUBJECT}")')
    
    if status == "OK":
        email_ids = messages[0].split()
        if not email_ids:
            print(f"❌ No emails found with subject '{SEARCH_SUBJECT}'")
        else:
            print(f"✅ Found {len(email_ids)} matching emails. Processing latest...")
            
    mail.logout()
    push_test_record()

if __name__ == "__main__":
    process_email()
