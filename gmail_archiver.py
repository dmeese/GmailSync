import os.path
import argparse
from tqdm import tqdm
import time
import re
import base64
from datetime import datetime

from bs4 import BeautifulSoup
from googleapiclient.errors import HttpError

# Import the shared utility functions
from gmail_utils import execute_batch_with_backoff, add_common_gmail_args, initialize_gmail_service

# This script only needs to read emails, so a readonly scope is sufficient and safer.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_message_body(payload):
    """
    Recursively parses a message payload to find the text/plain body.
    If only HTML is available, it's cleaned and returned as text.
    Returns the decoded text content.
    """
    if 'parts' in payload:
        # First, look for a text/plain part
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
        
        # If no text/plain, recurse into multipart parts
        for part in payload['parts']:
            if 'parts' in part:
                body = get_message_body(part)
                if body:
                    return body
        
        # If still no text/plain, fall back to the first text/html part (better than nothing)
        for part in payload['parts']:
            if part['mimeType'] == 'text/html' and 'data' in part['body']:                
                html_content = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                soup = BeautifulSoup(html_content, "html.parser")
                return soup.get_text(separator='\n', strip=True)

    elif 'body' in payload and 'data' in payload['body']:
        # For non-multipart messages
        if payload['mimeType'] == 'text/plain':
            return base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        elif payload['mimeType'] == 'text/html':
            html_content = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, "html.parser")
            return soup.get_text(separator='\n', strip=True)

    return "" # Return empty string if no body is found

def find_messages_in_date_range(service, start_date, end_date):
    """
    Fetches all message IDs within a specific date range.
    """
    all_message_ids = []
    # Gmail API uses YYYY/MM/DD format for date queries
    query = f"after:{start_date.replace('-', '/')} before:{end_date.replace('-', '/')}"
    print(f"Searching for messages with query: '{query}'")
    
    try:
        request = service.users().messages().list(userId='me', q=query, maxResults=500)
        
        with tqdm(desc="Fetching message pages") as pbar:
            while request is not None:
                response = request.execute()
                messages = response.get('messages', [])
                if messages:
                    all_message_ids.extend([m['id'] for m in messages])
                
                pbar.update(1)
                request = service.users().messages().list_next(previous_request=request, previous_response=response)
                time.sleep(0.1)

    except HttpError as error:
        print(f"An error occurred while fetching message IDs: {error}")
    
    print(f"\nFound a total of {len(all_message_ids)} messages in the specified date range.")
    return all_message_ids

def fetch_and_save_messages(service, message_ids, output_file):
    """
    Fetches the full content for a list of message IDs and saves them to a text file.
    """
    
    with open(output_file, 'w', encoding='utf-8') as f:
        
        def process_message_callback(request_id, response, exception):
            if exception:
                print(f"Error fetching details for request_id {request_id}: {exception}")
            else:
                message_id = response['id']
                payload = response['payload']
                headers = payload['headers']
                
                from_header = next((h['value'] for h in headers if h['name'].lower() == 'from'), 'N/A')
                subject_header = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'N/A')
                date_header = next((h['value'] for h in headers if h['name'].lower() == 'date'), 'N/A')
                
                body = get_message_body(payload)
                
                f.write("--- MESSAGE START ---\n")
                f.write(f"Message-ID: {message_id}\n")
                f.write(f"From: {from_header}\n")
                f.write(f"Subject: {subject_header}\n")
                f.write(f"Date: {date_header}\n\n")
                f.write(body)
                f.write("\n--- MESSAGE END ---\n\n")

        batch_size = 25  # A conservative batch size to avoid concurrency errors
        for i in tqdm(range(0, len(message_ids), batch_size), desc="Downloading Messages"):
            batch = service.new_batch_http_request(callback=process_message_callback)
            chunk = message_ids[i:i + batch_size]
            for message_id in chunk:
                # Request 'full' format to get the body
                batch.add(service.users().messages().get(userId='me', id=message_id, format='full'))
            execute_batch_with_backoff(batch)
            time.sleep(0.5)  # Pause to be a good API citizen

def valid_date(s):
    """Helper function to validate date format for argparse."""
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        msg = f"Not a valid date: '{s}'. Please use YYYY-MM-DD format."
        raise argparse.ArgumentTypeError(msg)

def main():
    """Main function to run the Gmail archiver."""
    parser = argparse.ArgumentParser(description="Archive Gmail messages from a specific date range to a text file.")
    add_common_gmail_args(parser)
    parser.add_argument(
        '--start-date',
        required=True,
        type=valid_date,
        help="The start date for the email search, in YYYY-MM-DD format."
    )
    parser.add_argument(
        '--end-date',
        required=True,
        type=valid_date,
        help="The end date for the email search, in YYYY-MM-DD format. To get emails older than 5 years, set this to 5 years ago."
    )
    parser.add_argument(
        '--output',
        default='email_archive.txt',
        help="Name of the output text file."
    )
    args = parser.parse_args()

    print("--- Gmail Archiver ---")
    
    service = initialize_gmail_service(args, SCOPES)
    if not service:
        return

    message_ids = find_messages_in_date_range(service, args.start_date, args.end_date)
    if not message_ids:
        print("No messages found in the specified date range. Exiting.")
        return
        
    fetch_and_save_messages(service, message_ids, args.output)
    
    print(f"\n--- Archiving Complete ---")
    print(f"All messages from the date range have been saved to {args.output}")

if __name__ == '__main__':
    main()
