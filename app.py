#!/usr/bin/env python3
"""
Combined Streamlit App for Gmail to Drive and PDF to Excel Workflows
Combines Gmail attachment downloader and LlamaParse PDF processor with real-time tracking
"""

import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from io import StringIO, BytesIO
import threading
import queue
import re
import warnings

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# Add LlamaParse import
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False

warnings.filterwarnings("ignore")

# Configure Streamlit page
st.set_page_config(
    page_title="Combined Automation Workflows",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

class CombinedAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        self.logs: List[Dict] = []
    
    def log(self, message: str, level: str = "INFO"):
        """Add log entry with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logs.append({
            "timestamp": timestamp, 
            "level": level.upper(), 
            "message": message
        })
        
        # Keep only last 100 logs to prevent memory issues
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]
    
    def authenticate_from_secrets(self, progress_bar, status_text):
        """Authenticate using Streamlit secrets with web-based OAuth flow"""
        try:
            self.log("Starting authentication process...", "INFO")
            status_text.text("Authenticating with Google APIs...")
            progress_bar.progress(10)
            
            # Check for existing token in session state
            if 'oauth_token' in st.session_state:
                try:
                    combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                    if creds and creds.valid:
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        self.log("Authentication successful using cached token!", "SUCCESS")
                        status_text.text("Authentication successful!")
                        return True
                    elif creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        self.log("Authentication successful after token refresh!", "SUCCESS")
                        status_text.text("Authentication successful!")
                        return True
                except Exception as e:
                    self.log(f"Cached token invalid: {str(e)}", "WARNING")
            
            # Use Streamlit secrets for OAuth
            if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
                creds_data = json.loads(st.secrets["google"]["credentials_json"])
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                
                # Configure for web application
                flow = Flow.from_client_config(
                    client_config=creds_data,
                    scopes=combined_scopes,
                    redirect_uri=st.secrets.get("redirect_uri", "https://mrl-auto-grn.streamlit.app/")
                )
                
                # Generate authorization URL
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                # Check for callback code
                query_params = st.query_params
                if "code" in query_params:
                    try:
                        code = query_params["code"]
                        flow.fetch_token(code=code)
                        creds = flow.credentials
                        
                        # Save credentials in session state
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        
                        progress_bar.progress(100)
                        self.log("OAuth authentication successful!", "SUCCESS")
                        status_text.text("Authentication successful!")
                        
                        # Clear the code from URL
                        st.query_params.clear()
                        return True
                    except Exception as e:
                        self.log(f"OAuth authentication failed: {str(e)}", "ERROR")
                        st.error(f"Authentication failed: {str(e)}")
                        return False
                else:
                    # Show authorization link
                    st.markdown("### Google Authentication Required")
                    st.markdown(f"[Click here to authorize with Google]({auth_url})")
                    self.log("Waiting for user to authorize application", "INFO")
                    st.info("Click the link above to authorize, you'll be redirected back automatically")
                    st.stop()
            else:
                self.log("Google credentials missing in Streamlit secrets", "ERROR")
                st.error("Google credentials missing in Streamlit secrets")
                return False
                
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            st.error(f"Authentication failed: {str(e)}")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            # Build search query
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            # Add date filter
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"Gmail search query: {query}", "INFO")
            
            # Execute search
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"Found {len(messages)} emails matching criteria", "SUCCESS")
            
            return messages
            
        except Exception as e:
            self.log(f"Gmail search failed: {str(e)}", "ERROR")
            return []
    
    def process_gmail_workflow(self, config: dict, progress_callback=None, status_callback=None):
        """Process Gmail attachment download workflow"""
        try:
            if status_callback:
                status_callback("Starting Gmail workflow...")
            
            self.log("Starting Gmail to Drive workflow", "INFO")
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            if progress_callback:
                progress_callback(25)
            
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {'success': True, 'processed': 0}
            
            if status_callback:
                status_callback(f"Found {len(emails)} emails. Processing attachments...")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                self.log("Failed to create base folder in Google Drive", "ERROR")
                return {'success': False, 'processed': 0}
            
            if progress_callback:
                progress_callback(50)
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                try:
                    if status_callback:
                        status_callback(f"Processing email {i+1}/{len(emails)}")
                    
                    # Get email details first
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self.log(f"Processing email: {subject} from {sender}", "INFO")
                    
                    # Get full message with payload
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        self.log(f"No payload found for email: {subject}", "WARNING")
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], sender, config, base_folder_id
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self.log(f"Found {attachment_count} attachments in: {subject}", "SUCCESS")
                    
                    if progress_callback:
                        progress = 50 + (i + 1) / len(emails) * 45
                        progress_callback(int(progress))
                    
                except Exception as e:
                    self.log(f"Failed to process email {email.get('id', 'unknown')}: {str(e)}", "ERROR")
            
            if progress_callback:
                progress_callback(100)
            
            if status_callback:
                status_callback(f"Gmail workflow completed! Processed {total_attachments} attachments")
            
            self.log(f"Gmail workflow completed. Processed {total_attachments} attachments from {processed_count} emails", "SUCCESS")
            
            return {'success': True, 'processed': total_attachments}
            
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0}
    
    def process_pdf_workflow(self, config: dict, progress_callback=None, status_callback=None):
        """Process PDF to Excel workflow using LlamaParse"""
        try:
            if not LLAMA_AVAILABLE:
                self.log("LlamaParse not available. Install with: pip install llama-cloud-services", "ERROR")
                return {'success': False, 'processed': 0}
            
            if status_callback:
                status_callback("Starting PDF to Excel workflow...")
            
            self.log("Starting PDF to Excel workflow with LlamaParse", "INFO")
            
            # Set up LlamaParse
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                self.log(f"Could not find LlamaParse agent '{config['llama_agent']}'", "ERROR")
                return {'success': False, 'processed': 0}
            
            self.log("LlamaParse agent found successfully", "SUCCESS")
            
            # Get PDF files from Drive
            pdf_files = self._list_drive_pdfs(
                config['drive_folder_id'], 
                config['days_back']
            )
            
            if progress_callback:
                progress_callback(25)
            
            if not pdf_files:
                self.log("No PDF files found in the specified folder", "WARNING")
                return {'success': True, 'processed': 0}
            
            if status_callback:
                status_callback(f"Found {len(pdf_files)} PDF files. Processing...")
            
            self.log(f"Found {len(pdf_files)} PDF files to process", "INFO")
            
            processed_count = 0
            total_rows = 0
            existing_headers = None
            
            for i, file in enumerate(pdf_files):
                try:
                    if status_callback:
                        status_callback(f"Processing PDF {i+1}/{len(pdf_files)}: {file['name']}")
                    
                    self.log(f"Processing PDF {i+1}/{len(pdf_files)}: {file['name']}", "INFO")
                    
                    # Download PDF from Drive
                    pdf_data = self._download_from_drive(file['id'], file['name'])
                    
                    if not pdf_data:
                        self.log(f"Failed to download PDF: {file['name']}", "ERROR")
                        continue
                    
                    # Save to temporary file for processing
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                        temp_file.write(pdf_data)
                        temp_path = temp_file.name
                    
                    try:
                        # Extract data with LlamaParse
                        result = self._safe_extract(agent, temp_path)
                        extracted_data = result.data
                        
                        # Clean up temp file
                        os.unlink(temp_path)
                        
                        # Flatten data for Google Sheets
                        rows = self._flatten_json(extracted_data)
                        for r in rows:
                            r["source_file"] = file['name']
                            r["processed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            r["drive_file_id"] = file['id']
                        
                        if rows:
                            # Prepare data for Google Sheets
                            self.log(f"Preparing {len(rows)} rows for Google Sheets from {file['name']}", "INFO")
                            
                            # Get all unique keys to create comprehensive headers
                            all_keys = set()
                            for row in rows:
                                all_keys.update(row.keys())
                            
                            # Use existing headers if available, otherwise create new ones
                            if existing_headers:
                                headers = existing_headers
                                # Add any missing headers
                                for key in all_keys:
                                    if key not in headers:
                                        headers.append(key)
                            else:
                                headers = list(all_keys)
                                existing_headers = headers
                            
                            # Convert to list of lists for Sheets API
                            values = []
                            if processed_count == 0:  # First file - include headers
                                values.append(headers)
                            
                            for row in rows:
                                row_values = [row.get(h, "") for h in headers]
                                values.append(row_values)
                            
                            # Append to Google Sheet
                            success = self._append_to_google_sheet(
                                config['spreadsheet_id'], 
                                config['sheet_range'], 
                                values
                            )
                            
                            if success:
                                total_rows += len(rows)
                                self.log(f"Successfully appended {len(rows)} rows from {file['name']}", "SUCCESS")
                            else:
                                self.log(f"Failed to update Google Sheet for {file['name']}", "ERROR")
                        
                        processed_count += 1
                        
                    except Exception as e:
                        # Clean up temp file in case of error
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)
                        raise e
                    
                    if progress_callback:
                        progress = 25 + (i + 1) / len(pdf_files) * 70
                        progress_callback(int(progress))
                    
                except Exception as e:
                    self.log(f"Failed to process PDF {file['name']}: {str(e)}", "ERROR")
            
            if progress_callback:
                progress_callback(100)
            
            if status_callback:
                status_callback(f"PDF workflow completed! Processed {processed_count} files")
            
            self.log(f"PDF workflow completed. Processed {processed_count} PDFs, added {total_rows} rows", "SUCCESS")
            
            return {'success': True, 'processed': processed_count, 'rows_added': total_rows}
            
        except Exception as e:
            self.log(f"PDF workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0}
    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"Failed to get email details for {message_id}: {str(e)}", "ERROR")
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            self.log(f"Failed to create folder {folder_name}: {str(e)}", "ERROR")
            return ""
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames to be safe for all operating systems"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _classify_extension(self, filename: str) -> str:
        """Categorize file by extension"""
        if not filename or '.' not in filename:
            return "Other"
            
        ext = filename.split(".")[-1].lower()
        
        type_map = {
            "pdf": "PDFs",
            "doc": "Documents", "docx": "Documents", "txt": "Documents",
            "xls": "Spreadsheets", "xlsx": "Spreadsheets", "csv": "Spreadsheets",
            "jpg": "Images", "jpeg": "Images", "png": "Images", "gif": "Images",
            "ppt": "Presentations", "pptx": "Presentations",
            "zip": "Archives", "rar": "Archives", "7z": "Archives",
        }
        
        return type_map.get(ext, "Other")
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender: str, config: dict, base_folder_id: str) -> int:
        """Recursively extract all attachments from an email"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, sender, config, base_folder_id
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                if not att.get("data"):
                    return 0
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create nested folder structure
                sender_email = sender
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                
                sender_folder_name = self._sanitize_filename(sender_email)
                search_term = config.get('search_term', 'all-attachments')
                search_folder_name = search_term if search_term else "all-attachments"
                file_type_folder = self._classify_extension(filename)
                
                # Create folder hierarchy
                sender_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                search_folder_id = self._create_drive_folder(search_folder_name, sender_folder_id)
                type_folder_id = self._create_drive_folder(file_type_folder, search_folder_id)
                
                # Upload file
                final_filename = self._sanitize_filename(filename)
                
                # Check if file already exists
                query = f"name='{final_filename}' and '{type_folder_id}' in parents and trashed=false"
                existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
                files = existing.get('files', [])
                
                if files:
                    self.log(f"File already exists, skipping: {filename}", "INFO")
                    return 1  # Count as processed but skipped
                
                file_metadata = {
                    'name': final_filename,
                    'parents': [type_folder_id]
                }
                
                media = MediaIoBaseUpload(
                    BytesIO(file_data),
                    mimetype='application/octet-stream',
                    resumable=True
                )
                
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                self.log(f"Uploaded to Drive: {filename}", "SUCCESS")
                processed_count += 1
                
            except Exception as e:
                self.log(f"Failed to process attachment {filename}: {str(e)}", "ERROR")
        
        return processed_count
    
    def _list_drive_pdfs(self, folder_id: str, days_back: int) -> List[Dict]:
        """List all PDF files in a Google Drive folder, optionally filtered by days back"""
        try:
            # Base query to find all PDF files in the specified folder
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
            
            if days_back is not None:
                today_utc = datetime.now(timezone.utc)
                start_date = today_utc - timedelta(days=days_back - 1)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                start_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                query += f" and createdTime >= '{start_str}'"
            
            files = []
            page_token = None

            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageToken=page_token,
                    pageSize=100
                ).execute()
                
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken', None)
                
                if page_token is None:
                    break

            self.log(f"Found {len(files)} PDF files in Drive folder", "SUCCESS")
            return files

        except Exception as e:
            self.log(f"Failed to list PDF files in folder: {str(e)}", "ERROR")
            return []
    
    def _download_from_drive(self, file_id: str, file_name: str) -> bytes:
        """Download a file from Google Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = request.execute()
            self.log(f"Downloaded from Drive: {file_name}", "SUCCESS")
            return file_data
        except Exception as e:
            self.log(f"Failed to download {file_name}: {str(e)}", "ERROR")
            return b""
    
    def _safe_extract(self, agent, file_path: str, retries: int = 3, wait_time: int = 2):
        """Retry-safe extraction to handle server disconnections"""
        for attempt in range(1, retries + 1):
            try:
                self.log(f"Extracting data (attempt {attempt}/{retries})...", "INFO")
                result = agent.extract(file_path)
                self.log("LlamaParse extraction successful", "SUCCESS")
                return result
            except Exception as e:
                self.log(f"Attempt {attempt} failed: {e}", "WARNING")
                time.sleep(wait_time)
        raise Exception(f"Extraction failed after {retries} attempts")
    
    def _flatten_json(self, extracted_data: Dict) -> List[Dict]:
        """Convert extracted_data into row format for Google Sheets"""
        flat_header = {
            "grn_date": extracted_data.get("grn_date", ""),
            "po_number": extracted_data.get("po_number", ""),
            "vendor_invoice_number": extracted_data.get("vendor_invoice_number", ""),
            "supplier": extracted_data.get("supplier", ""),
            "shipping_address": extracted_data.get("shipping_address", "")
        }

        merged_rows = []
        for item in extracted_data.get("items", []):
            clean_item = {k: self._clean_number(v) for k, v in item.items()}
            merged_row = {**flat_header, **clean_item}
            merged_rows.append(merged_row)

        return merged_rows
    
    def _clean_number(self, val):
        """Round floats to 2 decimals, keep integers as-is"""
        if isinstance(val, float):
            return round(val, 2)
        return val
    
    def _append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]):
        """Append data to a Google Sheet"""
        try:
            body = {
                'values': values
            }
            
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, 
                range=range_name,
                valueInputOption='USER_ENTERED', 
                body=body
            ).execute()
            
            updated_cells = result.get('updates', {}).get('updatedCells', 0)
            self.log(f"Appended {updated_cells} cells to Google Sheet", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Failed to append to Google Sheet: {str(e)}", "ERROR")
            return False


def create_streamlit_ui():
    """Create the Streamlit user interface"""
    st.title("🤖 Combined Automation Workflows")
    st.markdown("### Gmail to Drive & PDF to Excel Processing")
    
    # Initialize automation object
    if 'automation' not in st.session_state:
        st.session_state.automation = CombinedAutomation()
    
    # Sidebar for authentication
    st.sidebar.title("🔐 Authentication")
    
    if st.sidebar.button("Authenticate Google APIs", key="auth_button"):
        with st.spinner("Authenticating..."):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            success = st.session_state.automation.authenticate_from_secrets(progress_bar, status_text)
            
            if success:
                st.sidebar.success("Authentication successful!")
                st.session_state.authenticated = True
            else:
                st.sidebar.error("Authentication failed")
                st.session_state.authenticated = False
    
    # Check if authenticated
    if not st.session_state.get('authenticated', False):
        st.warning("Please authenticate with Google APIs first using the sidebar")
        st.stop()
    
    st.sidebar.success("Authenticated")
    
    # Configuration section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Configuration")
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        days_back = st.number_input(
            "Days Back",
            min_value=1,
            max_value=365,
            value=7,
            help="How many days back to search"
        )
    
    with col2:
        max_results = st.number_input(
            "Max Results",
            min_value=1,
            max_value=1000,
            value=50,
            help="Maximum number of items to process"
        )
    
    # Hardcoded configurations
    gmail_config = {
        'sender': 'aws-reports@moreretail.in',
        'search_term': 'in:spam',
        'days_back': days_back,
        'max_results': max_results,
        'gdrive_folder_id': '1gZoNjdGarwMD5-Ci3uoqjNZZ8bTNyVoy'
    }
    
    pdf_config = {
        'drive_folder_id': '1XHIFX-Gsb_Mx_AYjoi2NG1vMlvNE5CmQ',
        'llama_api_key': 'llx-DkwQuIwq5RVZk247W0r5WCdywejPI5CybuTDJgAUUcZKNq0A',
        'llama_agent': 'More retail Agent',
        'spreadsheet_id': '16y9DAK2tVHgnZNnPeRoSSPPE2NcspW_qqMF8ZR8OOC0',
        'sheet_range': 'mraws',
        'days_back': days_back
    }
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Gmail to Drive", "PDF to Excel", "Combined Workflow", "Logs"])
    
    with tab1:
        st.header("Gmail Attachment Downloader")
        st.markdown("**Configuration:**")
        st.markdown(f"- **Sender:** {gmail_config['sender']}")
        st.markdown(f"- **Search Term:** {gmail_config['search_term']}")
        st.markdown(f"- **Days Back:** {days_back}")
        st.markdown(f"- **Max Results:** {max_results}")
        
        if st.button("Start Gmail Workflow", type="primary", key="gmail_start"):
            st.session_state.workflow_running = True
            with st.spinner("Processing Gmail workflow..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                result = st.session_state.automation.process_gmail_workflow(
                    gmail_config, 
                    progress_callback=progress_bar.progress,
                    status_callback=status_text.text
                )
                
                st.session_state.workflow_running = False
                
                if result['success']:
                    st.balloons()
                    st.success(f"Gmail workflow completed! Processed {result['processed']} attachments")
                else:
                    st.error("Gmail workflow failed")
    
    with tab2:
        st.header("PDF to Excel Processor")
        st.markdown("**Configuration:**")
        st.markdown(f"- **Drive Folder ID:** {pdf_config['drive_folder_id']}")
        st.markdown(f"- **LlamaParse Agent:** {pdf_config['llama_agent']}")
        st.markdown(f"- **Spreadsheet ID:** {pdf_config['spreadsheet_id']}")
        st.markdown(f"- **Days Back:** {days_back}")
        
        if not LLAMA_AVAILABLE:
            st.error("LlamaParse not available. Please install: pip install llama-cloud-services")
        else:
            if st.button("Start PDF Workflow", type="primary", key="pdf_start"):
                st.session_state.workflow_running = True
                with st.spinner("Processing PDF workflow..."):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    result = st.session_state.automation.process_pdf_workflow(
                        pdf_config,
                        progress_callback=progress_bar.progress,
                        status_callback=status_text.text
                    )
                    
                    st.session_state.workflow_running = False
                    
                    if result['success']:
                        st.balloons()
                        rows_added = result.get('rows_added', 0)
                        st.success(f"PDF workflow completed! Processed {result['processed']} files, added {rows_added} rows")
                    else:
                        st.error("PDF workflow failed")
    
    with tab3:
        st.header("Combined Workflow")
        st.markdown("**Process Order:**")
        st.markdown("1. Gmail to Drive workflow (download attachments)")
        st.markdown("2. PDF to Excel workflow (process PDFs with LlamaParse)")
        
        st.markdown("**Configuration:**")
        st.markdown(f"- **Gmail Sender:** {gmail_config['sender']}")
        st.markdown(f"- **Gmail Search:** {gmail_config['search_term']}")
        st.markdown(f"- **PDF Agent:** {pdf_config['llama_agent']}")
        st.markdown(f"- **Days Back:** {days_back}")
        st.markdown(f"- **Max Results:** {max_results}")
        
        if not LLAMA_AVAILABLE:
            st.error("LlamaParse not available for PDF processing. Please install: pip install llama-cloud-services")
        else:
            if st.button("Start Combined Workflow", type="primary", key="combined_start"):
                st.session_state.workflow_running = True
                with st.spinner("Processing combined workflow..."):
                    overall_progress = st.progress(0)
                    status_text = st.empty()
                    
                    # Step 1: Gmail workflow
                    st.markdown("### Step 1: Gmail to Drive")
                    gmail_progress = st.progress(0)
                    gmail_status = st.empty()
                    
                    gmail_result = st.session_state.automation.process_gmail_workflow(
                        gmail_config,
                        progress_callback=gmail_progress.progress,
                        status_callback=gmail_status.text
                    )
                    
                    overall_progress.progress(50)
                    
                    if gmail_result['success']:
                        st.success(f"Gmail workflow completed! Processed {gmail_result['processed']} attachments")
                        
                        # Step 2: PDF workflow
                        st.markdown("### Step 2: PDF to Excel")
                        pdf_progress = st.progress(0)
                        pdf_status = st.empty()
                        
                        pdf_result = st.session_state.automation.process_pdf_workflow(
                            pdf_config,
                            progress_callback=pdf_progress.progress,
                            status_callback=pdf_status.text
                        )
                        
                        overall_progress.progress(100)
                        
                        if pdf_result['success']:
                            st.balloons()
                            rows_added = pdf_result.get('rows_added', 0)
                            st.success(f"Combined workflow completed successfully!")
                            st.info(f"Gmail: {gmail_result['processed']} attachments processed")
                            st.info(f"PDF: {pdf_result['processed']} files processed, {rows_added} rows added")
                        else:
                            st.error("PDF workflow failed")
                    else:
                        st.error("Gmail workflow failed - stopping combined workflow")
                    
                    st.session_state.workflow_running = False
    
    with tab4:
        st.header("Activity Logs")
        
        # Control buttons
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("Refresh Logs", key="refresh_logs"):
                st.rerun()
        
        with col2:
            if st.button("Clear Logs", key="clear_logs"):
                st.session_state.automation.logs = []
                st.success("Logs cleared!")
                time.sleep(1)
                st.rerun()
        
        with col3:
            # Auto-refresh toggle
            auto_refresh = st.checkbox("Auto-refresh logs (5s)", 
                                     value=st.session_state.get('auto_refresh_logs', False))
            st.session_state.auto_refresh_logs = auto_refresh
        
        st.markdown("---")
        
        # Display logs
        logs = st.session_state.automation.logs
        if logs:
            # Create a container for logs that will be updated
            log_container = st.container()
            
            with log_container:
                # Show recent logs first (reversed order)
                recent_logs = list(reversed(logs[-50:]))  # Show last 50 logs, most recent first
                
                for log in recent_logs:
                    level = log['level']
                    timestamp = log['timestamp']
                    message = log['message']
                    
                    # Create a formatted log entry
                    log_entry = f"**[{timestamp}]** {message}"
                    
                    if level == "ERROR":
                        st.error(log_entry, icon="❌")
                    elif level == "WARNING":
                        st.warning(log_entry, icon="⚠️")
                    elif level == "SUCCESS":
                        st.success(log_entry, icon="✅")
                    else:
                        st.info(log_entry, icon="ℹ️")
                
                # Show log count
                st.caption(f"Showing {len(recent_logs)} of {len(logs)} total logs")
        else:
            st.info("No logs available. Start a workflow to see activity logs.")
        
        # Auto-refresh functionality
        if auto_refresh and not st.session_state.get('workflow_running', False):
            # Only auto-refresh if no workflow is currently running to avoid conflicts
            time.sleep(5)
            st.rerun()


def create_help_section():
    """Create help section with instructions"""
    with st.sidebar.expander("Help & Instructions", expanded=False):
        st.markdown("""
        ### Setup Steps:
        1. **Authenticate** with Google APIs using the button above
        2. **Configure** Days Back and Max Results as needed
        3. **Choose a workflow** from the tabs:
           - **Gmail to Drive**: Downloads attachments from Gmail to Google Drive
           - **PDF to Excel**: Processes PDFs using LlamaParse and saves to Google Sheets
           - **Combined**: Runs both workflows in sequence
        4. **Monitor progress** in the Logs tab
        
        ### Configurations (Hardcoded):
        **Gmail Workflow:**
        - Sender: aws-reports@moreretail.in
        - Search: in:spam
        - Drive Folder: Configured automatically
        
        **PDF Workflow:**
        - LlamaParse Agent: More retail Agent
        - Drive Folder: Configured for PDF processing
        - Output: Google Sheets (mraws)
        
        ### Notes:
        - All configurations are pre-set except Days Back and Max Results
        - Combined workflow runs Gmail first, then PDF processing
        - Logs update in real-time during workflow execution
        - Files are organized automatically in Google Drive folders
        """)
    
    with st.sidebar.expander("About", expanded=False):
        st.markdown("""
        **Combined Automation Workflows v1.0**
        
        This application combines:
        - Gmail attachment downloading
        - PDF processing with LlamaParse
        - Google Drive organization
        - Google Sheets data consolidation
        
        Built with Streamlit and Google APIs.
        """)


def main():
    """Main function to run the Streamlit app"""
    try:
        # Initialize session state
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        
        if 'workflow_running' not in st.session_state:
            st.session_state.workflow_running = False
        
        if 'auto_refresh_logs' not in st.session_state:
            st.session_state.auto_refresh_logs = False
        
        # Create UI
        create_streamlit_ui()
        create_help_section()
        
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        st.info("Please refresh the page and try again.")


if __name__ == "__main__":
    main()
