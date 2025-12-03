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
        return text.strip().replace('\xa0', ' ')
    return ""

def identify_columns(header_row):
    """
    Tries to map column names to indices (0, 1, 2...)
    Returns a dictionary like: {'date': 0, 'store': 4, ...}
    """
    cols = header_row.find_all(['th', 'td'])
    mapping = {}
    
    print(f"   --- Analyzing Header Row ({len(cols)} columns) ---")
    for idx, col in enumerate(cols):
        text = clean_text(col.text).lower()
        print(f"   [Col {idx}]: {text}")
        
        if 'date' in text: mapping['date'] = idx
        elif 'time' in text: mapping['time'] = idx
        elif 'call' in text or 'phone' in text or 'number' in text: mapping['caller'] = idx
        elif 'agent' in text or 'rep' in text: mapping['agent'] = idx
        elif 'store' in text or 'dealership' in text or 'loc' in text: mapping['store'] = idx
        elif 'status' in text or 'result' in text: mapping['status'] = idx
        elif 'duration' in text or 'length' in text: mapping['duration'] = idx
        elif 'note' in text or 'comment' in text: mapping['notes'] = idx

    print(f"   ✅ Mapped Columns: {mapping}")
    return mapping

def extract_call_data(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    rows_data = []
    
    all_rows = soup.find_all('tr')
    if not all_rows:
        return []

    # 1. Find the Header Row (First row with meaningful text)
    header_mapping = {}
    data_start_index = 0
    
    for i, row in enumerate(all_rows):
        # Look for a row that has "Date" or "Caller" in it to be the header
        text = row.text.lower()
        if 'date' in text or 'caller' in text or 'store' in text:
            header_mapping = identify_columns(row)
            data_start_index = i + 1
            break
    
    # Fallback: If no headers found, use default indices (Blind Guess)
    if not header_mapping:
        print("⚠️ No headers found! Using default column mapping.")
        header_mapping = {
            'date': 0, 'time': 1, 'caller': 2, 'agent': 3, 
            'store': 4, 'status': 5, 'duration': 6, 'notes': 7
        }

    # 2. Extract Data
    print(f"   - Scanning data starting from row {data_start_index}...")
    for row in all_rows[data_start_index:]:
        cols = row.find_all(['td', 'th'])
        
        # We need enough columns to match our largest mapped index
        max_index = max(header_mapping.values()) if header_mapping else 0
        if len(cols) <= max_index:
            continue

        # Extract Text & Links
        row_values = [clean_text(c.text) for c in cols]
        
        # Audio / CRM Links search
        audio_link = ""
        crm_link = ""
        for col in cols:
            link_tag = col.find('a', href=True)
            if link_tag:
                href = link_tag['href']
                if any(x in clean_text(col.text).lower() for x in ['listen', 'play']) or '.mp3' in href:
                    audio_link = href
                elif any(x in clean_text(col.text).lower() for x in ['view', 'crm']) or 'crm' in href:
                    crm_link = href

        # Build Record using Mapping
        # Use .get() with a default to avoid crashes if a column is missing
        call_record = {
            "date": row_values[header_mapping.get('date', 0)],
            "time": row_values[header_mapping.get('time', 1)],
            "caller": row_values[header_mapping.get('caller', 2)],
            "agent": row_values[header_mapping.get('agent', 3)],
            "store": row_values[header_mapping.get('store', 4)],
            "status": row_values[header_mapping.get('status', 5)],
            "duration": row_values[header_mapping.get('duration', 6)],
            "notes": row_values[header_mapping.get('notes', 7)] if len(row_values) > 7 else "",
            "audio_url": audio_link,
            "crm_url": crm_link
        }

        # Validate it looks like a real row (has a date number)
        if any(char.isdigit() for char in call_record['date']):
            rows_data.append(call_record)
            
    print(f"   - Extracted {len(rows_data)} valid calls.")
    return rows_data

def push_to_firestore(data):
    if not data:
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
    if not mail: return

    mail.select("inbox")
    print(f"--- SEARCHING FOR: {SEARCH_SUBJECT} ---")
    status, messages = mail.search(None, f'(SUBJECT "{SEARCH_SUBJECT}")')
    
    if status == "OK":
        email_ids = messages[0].split()
        if not email_ids:
            print(f"❌ No emails found.")
            return

        # Process LAST 5 emails to catch up on data
        latest_ids = email_ids[-5:] 
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
