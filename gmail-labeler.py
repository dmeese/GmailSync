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

# If modifying these scopes, delete the file token.json.
# The 'gmail.modify' scope is required to add labels to emails.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

def fetch_email_headers(service, max_results=100):
    """
    Fetches the headers of the most recent emails using an efficient batch request.
    """
    try:
        # Get a list of message IDs
        results = service.users().messages().list(userId='me', maxResults=max_results).execute()
        messages = results.get('messages', [])

        headers_list = []
        if not messages:
            print('No messages found.')
            return headers_list

        print(f"Fetching headers for {len(messages)} messages using batch requests...")

        def process_message_callback(request_id, response, exception):
            if exception:
                # Handle errors for individual requests, e.g., a single deleted message
                print(f"Error fetching message details: {exception}")
            else:
                headers = response['payload']['headers']
                headers_list.append({
                    'id': response['id'],
                    'snippet': response['snippet'],
                    'headers': headers
                })

        # Gmail API has a limit of 100 operations per batch request.
        # We chunk the messages into smaller lists (e.g., 50) to avoid concurrency limits.
        batch_size = 25 # Using a more conservative batch size to avoid concurrency errors
        for i in tqdm(range(0, len(messages), batch_size), desc="Fetching Email Headers"):
            batch = service.new_batch_http_request(callback=process_message_callback)
            chunk = messages[i:i + batch_size]
            for message in chunk:
                batch.add(service.users().messages().get(
                    userId='me', id=message['id'], format='metadata',
                    metadataHeaders=['Subject', 'From', 'Date', 'To', 'List-Unsubscribe']
                ))
            try:
                batch.execute()
                time.sleep(1)  # Add a 1-second pause to respect concurrency limits
            except HttpError as error:
                # This could happen if the entire batch fails for some reason
                print(f"An error occurred during a batch fetch execution: {error}")

        return headers_list

    except HttpError as error:
        print(f'An error occurred: {error}')
        return []

def parse_headers_to_dataframe(headers_list):
    """
    Parses the list of headers into a pandas DataFrame for easy analysis.
    """
    parsed_data = []
    for item in headers_list:
        headers = item['headers']
        header_dict = { h['name']: h['value'] for h in headers }
        header_dict['id'] = item['id']
        header_dict['snippet'] = item['snippet']
        parsed_data.append(header_dict)
    
    df = pd.DataFrame(parsed_data)
    # Reorder columns for better readability
    cols = ['id', 'From', 'To', 'Subject', 'Date', 'List-Unsubscribe', 'snippet']
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols]
    return df

def extract_domain_from_sender(sender):
    """Extracts the domain from a 'From' header string."""
    if not isinstance(sender, str):
        return None
    
    # Find email address in angle brackets, e.g., "Sender Name" <sender@example.com>
    match = re.search(r'<([^>]+)>', sender)
    if match:
        email = match.group(1)
    else:
        # No angle brackets, use the whole string as the email
        email = sender.strip()
        
    if '@' in email:
        return email.split('@')[-1]
    
    return None

def ensure_label_exists(service, label_name, all_labels_map):
    """
    Checks if a label exists using a pre-fetched map.
    Creates it via API call if not found, and updates the map.
    """
    if label_name in all_labels_map:
        return all_labels_map[label_name]
    else:
        print(f"Label '{label_name}' not found locally, creating it via API...")
        label_body = {'name': label_name, 'labelListVisibility': 'labelShow', 'messageListVisibility': 'show'}
        try:
            label = service.users().labels().create(userId='me', body=label_body).execute()
            print(f"Label '{label_name}' created with ID: {label['id']}")
            # Update the map with the newly created label
            all_labels_map[label_name] = label['id']
            return label['id']
        except HttpError as error:
            print(f"An error occurred while creating label '{label_name}': {error}")
            return None

def apply_label_to_emails_batch(service, message_ids, label_id, label_name):
    """Applies a given label to a list of message IDs using efficient batch requests."""
    if not message_ids:
        print("No messages to label.")
        return

    print(f"Applying label '{label_name}' to {len(message_ids)} emails...")
    body = {'addLabelIds': [label_id], 'removeLabelIds': []}

    def batch_callback(request_id, response, exception):
        if exception:
            print(f"Error applying label for request {request_id}: {exception}")

    # Gmail API has a limit of 100 operations per batch request.
    # We chunk the message_ids into smaller lists (e.g., 50) to avoid concurrency limits.
    batch_size = 25 # Using a more conservative batch size to avoid concurrency errors
    for i in tqdm(range(0, len(message_ids), batch_size), desc="Labeling Emails in Batches"):
        batch = service.new_batch_http_request(callback=batch_callback)
        chunk = message_ids[i:i + batch_size]
        for message_id in chunk:
            batch.add(service.users().messages().modify(
                userId='me',
                id=message_id,
                body=body
            ))
        try:
            batch.execute()
            time.sleep(1)  # Add a 1-second pause to respect concurrency limits
        except HttpError as error:
            print(f"An error occurred during a batch execution: {error}")

def apply_domain_labels(service, df, parent_label_name, all_labels_map):
    """Groups emails by sender domain and applies a nested label."""
    
    # Ensure the parent label exists first.
    parent_label_id = ensure_label_exists(service, parent_label_name, all_labels_map)
    if not parent_label_id:
        print(f"Could not find or create parent label '{parent_label_name}'. Aborting labeling.")
        return
    
    # Group by the extracted domain
    domain_groups = df.groupby('domain')

    print(f"\nFound {len(domain_groups)} unique domains to label.")

    for domain, group in tqdm(domain_groups, desc="Applying Domain Labels"):
        if not domain:
            continue

        nested_label_name = f"{parent_label_name}/{domain}"
        
        # Check cache first, otherwise get from API and store it
        label_id = ensure_label_exists(service, nested_label_name, all_labels_map)
        if label_id:
            message_ids = group['id'].tolist()
            apply_label_to_emails_batch(service, message_ids, label_id, nested_label_name)

def main():
    """
    Main function to run the Gmail analyzer.
    """
    parser = argparse.ArgumentParser(description="Analyze Gmail inbox and optionally label emails.")
    parser.add_argument(
        '--label-unsubscribe',
        nargs='?',
        const='unsubscribe',
        default=None,
        metavar='PARENT_LABEL',
        help="Apply nested labels under a PARENT_LABEL. If no label name is provided, defaults to 'unsubscribe'."
    )
    parser.add_argument(
        '--creds',
        default='credentials.json',
        help="Path to credentials.json or a 1Password secret reference (e.g., 'op://vault/item/field')."
    )
    args = parser.parse_args()

    print("--- Gmail Analyzer ---")
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

    df = None  # Initialize df to ensure it's available in the finally block
    try:
        # You can change the number of emails to fetch here. Fetching headers is fast.
        email_headers = fetch_email_headers(service, max_results=500)
        
        if not email_headers:
            print("Could not fetch any email headers.")
            return

        df = parse_headers_to_dataframe(email_headers)
        
        print("\n--- Analysis Results ---")
        
        # Example Analysis: Top 20 Senders
        print("\nTop 20 Senders:")
        print(df['From'].value_counts().head(20))
        
        # Example Analysis: Senders of emails with Unsubscribe links
        unsubscribe_df = df[df['List-Unsubscribe'].notna()].copy()
        print(f"\nFound {len(unsubscribe_df)} emails with an unsubscribe link (likely newsletters/marketing).")
        if not unsubscribe_df.empty:
            print("Top Senders with Unsubscribe Links:")
            print(unsubscribe_df['From'].value_counts().head(10))

        # --- New Feature: Labeling ---
        if args.label_unsubscribe is not None:
            print("\n--- Labeling Mode Activated ---")
            print("WARNING: This will create nested labels and apply them to the recently analyzed emails.")
            if not unsubscribe_df.empty:
                # Add the domain column for labeling
                unsubscribe_df['domain'] = unsubscribe_df['From'].apply(extract_domain_from_sender)
                parent_label_name = args.label_unsubscribe
                
                # Fetch all labels once for efficiency
                print("\nFetching all existing labels from Gmail...")
                all_labels_results = service.users().labels().list(userId='me').execute()
                all_labels_map = {l['name']: l['id'] for l in all_labels_results.get('labels', [])}
                
                apply_domain_labels(service, unsubscribe_df, parent_label_name, all_labels_map)
            else:
                print("No emails with unsubscribe links found in the recent analysis to label.")

    finally:
        # Save to CSV if the DataFrame was successfully created
        if df is not None:
            output_filename = 'email_analysis.csv'
            df.to_csv(output_filename, index=False)
            print(f"\nFull analysis data saved to {output_filename}")


if __name__ == '__main__':
    main()
