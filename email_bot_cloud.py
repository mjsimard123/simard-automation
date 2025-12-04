import imaplib
import email
from email.header import decode_header
import firebase_admin
from firebase_admin import credentials, firestore
from bs4 import BeautifulSoup
import os
import hashlib
import datetime
import re

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

def clean_text(text):
    if text:
        return " ".join(text.split()).strip()
    return ""

def parse_friendly_date(date_str):
    """
    Converts 'Dec 2, 1:29 pm' -> '2025-12-02' and '01:29 PM'
    """
    try:
        current_year = datetime.datetime.now().year
        # Parse format like "Dec 2, 1:29 pm"
        dt = datetime.datetime.strptime(f"{current_year} {date_str}", "%Y %b %d, %I:%M %p")
        return dt.strftime("%Y-%m-%d"), dt.strftime("%I:%M %p")
    except:
        return date_str, ""

def extract_store_from_subject(subject):
    # Subject looks like: "Appt InSights > Simard Automotive 2025-12-02"
    try:
        if ">" in subject:
            parts = subject.split(">")
            store_part = parts[1].strip() # "Simard Automotive 2025-12-02"
            # Remove the date from the end if it exists
            store_name = re.sub(r'\d{4}-\d{2}-\d{2}', '', store_part).strip()
            return store_name
    except:
        pass
    return "Simard Main" # Default

def extract_call_data(html_content, default_store):
    soup = BeautifulSoup(html_content, "html.parser")
    rows_data = []
    
    all_rows = soup.find_all('tr')
    
    # We look for the standard 7-column layout based on your report
    # 0: Advisor | 1: Caller | 2: Duration | 3: From | 4: Date | 5: Score | 6: Action
    
    for row in all_rows:
        cols = row.find_all(['td', 'th'])
        # Only process rows that have enough columns and look like data
        if len(cols) >= 5:
            row_text = [clean_text(c.text) for c in cols]
            
            # Skip header row
            if "Advisor" in row_text[0] or "Date" in row_text[4]:
                continue

            try:
                raw_date = row_text[4]
                iso_date, time_str = parse_friendly_date(raw_date)
                
                # Check for links in the last column (Action)
                details_link = ""
                last_col = cols[-1]
                link_tag = last_col.find('a', href=True)
                if link_tag:
                    details_link = link_tag['href']

                # Create the record
                call_record = {
                    "agent": row_text[0],       # Advisor
                    "caller": row_text[1],      # Caller Name
                    "duration": row_text[2],    # Duration
                    "phone": row_text[3],       # From Phone
                    "date": iso_date,           # YYYY-MM-DD
                    "time": time_str,           # HH:MM AM
                    "display_date": raw_date,   # Original text
                    "store": default_store,     # From Subject Line
                    "status": "Completed",      # Default status (since report doesn't specify)
                    "score": row_text[5],       # Score
                    "crm_url": details_link,    # Link to details
                    "audio_url": ""             # No direct audio link in this table format usually
                }
                
                # Validation: Date must look valid (contains 202)
                if "202" in iso_date:
                    rows_data.append(call_record)
            except Exception as e:
                # Skip malformed rows
                continue

    print(f"   - Extracted {len(rows_data)} calls.")
    return rows_data

def push_to_firestore(data):
    if not data: return
    
    collection_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('calls')
    
    count = 0
    for record in data:
        # Unique ID = Date + Time + Phone to avoid duplicates
        unique_string = f"{record['date']}{record['time']}{record['phone']}"
        doc_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        doc_ref = collection_ref.document(doc_id)
        doc_ref.set(record, merge=True)
        count += 1
            
    print(f"✅ Synced {count} records.")

def process_email():
    mail = connect_to_mail()
    if not mail: return

    mail.select("inbox")
    print(f"--- SEARCHING FOR: {SEARCH_SUBJECT} ---")
    status, messages = mail.search(None, f'(SUBJECT "{SEARCH_SUBJECT}")')
    
    if status == "OK":
        email_ids = messages[0].split()
        if not email_ids:
            print(f"❌ No emails found.")
            return

        # Process LAST 5 emails to fill DB with historical data
        latest_ids = email_ids[-5:] 
        print(f"✅ Found {len(email_ids)} emails. Processing last {len(latest_ids)}...")
        
        for e_id in latest_ids:
            _, msg_data = mail.fetch(e_id, "(RFC822)")
            for response in msg_data:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])
                    
                    # Extract Store from Subject
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")
                    
                    store_name = extract_store_from_subject(subject)
                    print(f"   Processing Email: {subject} -> Store: {store_name}")

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
                        data = extract_call_data(body, store_name)
                        push_to_firestore(data)
    mail.logout()

if __name__ == "__main__":
    process_email()
