import os.path
import pandas as pd
import argparse
from tqdm import tqdm
import time
import re

from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

# Import the shared utility functions
from gmail_utils import get_gmail_service, get_secret_from_1password

# This script only needs to read emails, so a readonly scope is sufficient and safer.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def extract_email_from_sender(sender):
    """Extracts the clean email address from a 'From' header string."""
    if not isinstance(sender, str):
        return None
    
    match = re.search(r'<([^>]+)>', sender)
    if match:
        email = match.group(1)
    else:
        email = sender.strip()
        
    if '@' in email and '.' in email.split('@')[-1]:
        return email.lower()
    
    return sender.lower()

def get_all_message_ids(service, limit=None):
    """
    Fetches all message IDs from the user's mailbox.
    Handles pagination to retrieve all messages.
    """
    all_message_ids = []
    try:
        request = service.users().messages().list(userId='me', maxResults=500)
        
        with tqdm(desc="Fetching message pages") as pbar:
            while request is not None:
                response = request.execute()
                messages = response.get('messages', [])
                if messages:
                    all_message_ids.extend([m['id'] for m in messages])
                
                # If a limit is set, check if we have enough messages
                if limit and len(all_message_ids) >= limit:
                    break

                pbar.update(1)
                request = service.users().messages().list_next(previous_request=request, previous_response=response)
                time.sleep(0.1)

    except HttpError as error:
        print(f"An error occurred while fetching message IDs: {error}")

    if limit:
        all_message_ids = all_message_ids[:limit]
    print(f"\nFound {len(all_message_ids)} messages to process.")
    return all_message_ids

def fetch_senders_in_batches(service, message_ids):
    """
    Fetches the 'From' header for a list of message IDs using efficient batch requests.
    """
    senders_list = []
    
    def process_message_callback(request_id, response, exception):
        if exception:
            # This can happen with concurrent requests or if a message was deleted.
            # We'll print the error but continue processing.
            print(f"Error fetching details for request_id {request_id}: {exception}")
        else:
            headers = response['payload']['headers']
            from_header = next((h['value'] for h in headers if h['name'] == 'From'), None)
            if from_header:
                email = extract_email_from_sender(from_header)
                if email:
                    senders_list.append(email)

    batch_size = 25  # A very conservative batch size to avoid concurrency errors
    for i in tqdm(range(0, len(message_ids), batch_size), desc="Processing Messages"):
        batch = service.new_batch_http_request(callback=process_message_callback)
        chunk = message_ids[i:i + batch_size]
        for message_id in chunk:
            batch.add(service.users().messages().get(
                userId='me', id=message_id, format='metadata',
                metadataHeaders=['From']
            ))
        try:
            batch.execute()
            time.sleep(1) # Pause to respect concurrency limits
        except HttpError as error:
            print(f"An error occurred during a batch fetch execution: {error}")
            
    return senders_list

def main():
    """Main function to run the Gmail sender counter."""
    parser = argparse.ArgumentParser(description="Count all emails from each sender in your Gmail account.")
    parser.add_argument(
        '--creds',
        default='credentials.json',
        help="Path to credentials.json or a 1Password secret reference (e.g., 'op://vault/item/field')."
    )
    parser.add_argument(
        '--output',
        default='sender_counts.csv',
        help="Name of the output CSV file."
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help="Limit the scan to the N most recent emails. Scans all by default."
    )
    args = parser.parse_args()

    print("--- Gmail Sender Counter ---")
    if not args.limit:
        print("WARNING: This script will scan your ENTIRE mailbox, which may take a long time.")
    
    service = None
    creds_arg = args.creds
    if creds_arg.startswith("op://"):
        print("Fetching Gmail credentials from 1Password...")
        creds_content = get_secret_from_1password(creds_arg)
        if creds_content:
            service = get_gmail_service(SCOPES, credentials_json_content=creds_content)
    else:
        service = get_gmail_service(SCOPES, credentials_path=creds_arg)
    if not service:
        return

    all_ids = get_all_message_ids(service, limit=args.limit)
    if not all_ids:
        print("No message IDs found. Exiting.")
        return
        
    all_senders = fetch_senders_in_batches(service, all_ids)
    
    if not all_senders:
        print("Could not retrieve any sender information.")
        return
        
    print("\n--- Analysis Complete ---")
    
    sender_series = pd.Series(all_senders)
    sender_counts = sender_series.value_counts().reset_index()
    sender_counts.columns = ['sender_email', 'count']
    
    print("\nTop 25 Senders by Email Count:")
    print(sender_counts.head(25).to_string(index=False))
    
    try:
        sender_counts.to_csv(args.output, index=False)
        print(f"\nFull report saved to {args.output}")
    except Exception as e:
        print(f"\nError saving CSV file: {e}")

if __name__ == '__main__':
    main()