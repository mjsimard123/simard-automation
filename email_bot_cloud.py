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
    """Parses HTML table, extracting Text AND Links."""
    soup = BeautifulSoup(html_content, "html.parser")
    rows_data = []
    
    rows = soup.find_all('tr')
    print(f"   - Scanning {len(rows)} rows...")
    
    for row in rows:
        cols = row.find_all(['td', 'th'])
        
        # We need both text AND potential links from each column
        clean_cols = []
        audio_link = ""
        crm_link = ""
        
        for col in cols:
            text = col.text.strip()
            clean_cols.append(text)
            
            # Check for hidden links inside this column
            link_tag = col.find('a', href=True)
            if link_tag:
                href = link_tag['href']
                # If the text says "Listen" or it looks like audio, save it
                if "listen" in text.lower() or "play" in text.lower() or ".mp3" in href or ".wav" in href:
                    audio_link = href
                # If it's a CRM link (often on the Name or a 'View' button)
                elif "view" in text.lower() or "crm" in str(href):
                    crm_link = href

        # Ensure we have enough columns to be a valid record (at least 5)
        if len(clean_cols) >= 5: 
            # Check if it looks like a date row (has numbers and slashes/dashes)
            # This replaces the strict "202" check which might fail on "12/01/25"
            if any(char.isdigit() for char in clean_cols[0]):
                call_record = {
                    "date": clean_cols[0],
                    "time": clean_cols[1],
                    "caller": clean_cols[2],
                    "agent": clean_cols[3],
                    "store": clean_cols[4],
                    "status": clean_cols[5] if len(clean_cols) > 5 else "Unknown",
                    "duration": clean_cols[6] if len(clean_cols) > 6 else "",
                    "notes": clean_cols[7] if len(clean_cols) > 7 else "",
                    "audio_url": audio_link,  # NEW FIELD
                    "crm_url": crm_link       # NEW FIELD
                }
                rows_data.append(call_record)
            
    print(f"   - Extracted {len(rows_data)} valid calls.")
    return rows_data

def push_to_firestore(data):
    if not data:
        print("⚠️ No call data found in this email.")
        return
    
    collection_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('calls')
    
    count = 0
    for record in data:
        # Create unique ID
        unique_string = f"{record['date']}{record['time']}{record['caller']}"
        doc_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        doc_ref = collection_ref.document(doc_id)
        doc_ref.set(record, merge=True)
        count += 1
            
    print(f"✅ Synced {count} records to Firestore.")

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
            print(f"❌ No emails found.")
            return

        # Process the LAST 3 emails to ensure we catch recent data
        # (Processing more than 1 helps populate the "few things" issue)
        latest_ids = email_ids[-3:] 
        print(f"✅ Found {len(email_ids)} emails. Processing last {len(latest_ids)}...")
        
        for e_id in latest_ids:
            _, msg_data = mail.fetch(e_id, "(RFC822)")
            for response in msg_data:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])
                    
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
                        print(f"Processing email ID: {e_id}")
                        data = extract_call_data(body)
                        push_to_firestore(data)
                            
    mail.logout()

if __name__ == "__main__":
    process_email()
