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
    try:
        current_year = datetime.datetime.now().year
        dt = datetime.datetime.strptime(f"{current_year} {date_str}", "%Y %b %d, %I:%M %p")
        return dt.strftime("%Y-%m-%d"), dt.strftime("%I:%M %p")
    except:
        return date_str, ""

def determine_store_and_agent(raw_advisor_text):
    """
    Parses 'Ray . - 102' into Agent: 'Ray' and Store: 'Gaffney'
    """
    raw_lower = raw_advisor_text.lower()
    
    # 1. Extract Extension (look for 3 digits)
    ext_match = re.search(r'(\d{3})', raw_advisor_text)
    ext = ext_match.group(1) if ext_match else ""
    
    # 2. Clean up Agent Name (Remove digits, dots, dashes)
    # e.g. "Ray . - 102" -> "Ray"
    agent_name = re.sub(r'[\d\.\-]+', '', raw_advisor_text).strip()
    if not agent_name and ext: 
        agent_name = f"Advisor {ext}" # Fallback if name is empty
    
    # 3. Determine Store based on Rules
    store = "Unknown Store"
    
    # Rule Set A: Explicit Location Names in Text
    if "seward" in raw_lower: store = "Seward"
    elif "eagle" in raw_lower: store = "Eagle River"
    elif "airport" in raw_lower: store = "Airport"
    elif "cushman" in raw_lower: store = "Cushman"
    elif "m1" in raw_lower: store = "Steese" # Assuming M1 is Steese based on 500s
    
    # Rule Set B: Extension Ranges (overrides text if specific)
    elif ext:
        if ext.startswith('1'): store = "Gaffney"      # 100s -> Ray/Gaffney
        elif ext.startswith('2'): store = "Airport"    # 200s -> John/Airport
        elif ext.startswith('3'): store = "Cushman"    # 300s -> Jed/Cushman
        elif ext.startswith('4'): store = "Illinois"   # 400s -> Arthur/Illinois
        elif ext.startswith('5'): 
            # 500s are split
            if ext in ['531', '532']: store = "Eagle River"
            elif ext.startswith('52'): store = "Seward"
            else: store = "Steese" # 502, 503 fallback
            
    return agent_name, store

def extract_call_data(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    rows_data = []
    
    all_rows = soup.find_all('tr')
    
    # Columns in PDF: Advisor | Caller | Duration | From | Date | Score | Action
    
    for row in all_rows:
        cols = row.find_all(['td', 'th'])
        if len(cols) >= 6:
            row_text = [clean_text(c.text) for c in cols]
            
            # Skip Headers
            if "Advisor" in row_text[0] or "Date" in row_text[4]:
                continue

            try:
                # Basic Data
                raw_advisor = row_text[0]
                raw_date = row_text[4]
                
                # Intelligent Parsing
                agent_clean, store_clean = determine_store_and_agent(raw_advisor)
                iso_date, time_str = parse_friendly_date(raw_date)
                
                # Link Extraction
                details_link = ""
                last_col = cols[-1]
                link_tag = last_col.find('a', href=True)
                if link_tag: details_link = link_tag['href']

                # Create Record
                call_record = {
                    "agent": agent_clean,
                    "caller": row_text[1],
                    "duration": row_text[2],
                    "phone": row_text[3],
                    "date": iso_date,
                    "time": time_str,
                    "display_date": raw_date,
                    "store": store_clean,     # <--- Now Calculated Logic
                    "score": row_text[5],
                    "crm_url": details_link,
                    "audio_url": "",          # No audio links in this specific report format
                    "status": "Completed"
                }
                
                if "202" in iso_date:
                    rows_data.append(call_record)
            except Exception as e:
                continue

    print(f"   - Extracted {len(rows_data)} calls.")
    return rows_data

def push_to_firestore(data):
    if not data: return
    
    collection_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('calls')
    
    count = 0
    for record in data:
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
                        print(f"Processing Email ID: {e_id}")
                        data = extract_call_data(body)
                        push_to_firestore(data)
    mail.logout()

if __name__ == "__main__":
    process_email()
