import os.path
import subprocess
import time
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

def get_secret_from_1password(secret_reference):
    """Fetches a secret from 1Password using the op CLI."""
    try:
        # The --no-color flag ensures clean output
        result = subprocess.run(
            ['op', 'read', '--no-color', secret_reference],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except FileNotFoundError:
        print("\nError: 1Password CLI ('op') not found. Is it installed and in your system's PATH?")
        return None
    except subprocess.CalledProcessError as e:
        print(f"\nError fetching secret '{secret_reference}' from 1Password.")
        print(f"1Password CLI error: {e.stderr.strip()}")
        return None

def get_gmail_service(scopes, credentials_path='credentials.json', credentials_json_content=None):
    """
    Authenticates with the Gmail API and returns a service object.
    Handles the OAuth 2.0 flow and uses scope-specific token files.
    """
    creds = None
    # Create a unique token file name based on the scopes to avoid conflicts
    scope_name = "readonly" if 'readonly' in scopes[0] else "modify"
    token_path = f'token.{scope_name}.json'

    if os.path.exists(token_path):
        from google.oauth2.credentials import Credentials
        creds = Credentials.from_authorized_user_file(token_path, scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                from google.auth.transport.requests import Request
                print(f"Refreshing access token for {scope_name} scope...")
                creds.refresh(Request())
            except RefreshError as e:
                print(f"Error refreshing token: {e}. Deleting {token_path} and re-authenticating.")
                if os.path.exists(token_path):
                    os.remove(token_path)
                creds = None

        if not creds or not creds.valid:
            flow = None
            if credentials_json_content:
                try:
                    client_config = json.loads(credentials_json_content)
                    flow = InstalledAppFlow.from_client_config(client_config, scopes)
                except json.JSONDecodeError:
                    print("Error: The provided credentials content is not valid JSON.")
                    return None
            elif os.path.exists(credentials_path):
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scopes)
            else:
                print(f"Error: Credentials not found. Provide content directly or ensure '{credentials_path}' exists.")
                return None

            print(f"No valid token found for {scope_name} scope. Starting authentication flow...")
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('gmail', 'v1', credentials=creds)
        return service
    except HttpError as error:
        print(f'An error occurred: {error}')
        return None

def add_common_gmail_args(parser):
    """Adds common arguments for Gmail scripts to an argparse.ArgumentParser."""
    parser.add_argument(
        '--creds',
        default='credentials.json',
        help="Path to credentials.json or a 1Password secret reference (e.g., 'op://vault/item/field')."
    )

def initialize_gmail_service(args, scopes):
    """Initializes and returns the Gmail service object based on parsed arguments."""
    service = None
    creds_arg = args.creds
    if creds_arg.startswith("op://"):
        print("Fetching Gmail credentials from 1Password...")
        creds_content = get_secret_from_1password(creds_arg)
        if creds_content:
            service = get_gmail_service(scopes, credentials_json_content=creds_content)
    else:
        service = get_gmail_service(scopes, credentials_path=creds_arg)
    
    if not service:
        print("Failed to initialize Gmail service.")
    
    return service

def execute_batch_with_backoff(batch, max_retries=5, initial_delay=1.0, backoff_factor=2.0):
    """
    Executes a Google API batch request with exponential backoff for retries.

    Args:
        batch: The BatchHttpRequest object to execute.
        max_retries (int): The maximum number of times to retry the request.
        initial_delay (float): The initial delay in seconds before the first retry.
        backoff_factor (float): The factor by which to multiply the delay for each subsequent retry.
    """
    for attempt in range(max_retries):
        try:
            batch.execute()
            return  # Success, exit the function
        except HttpError as error:
            # Check if the error is a rate limit error (403/429) or a server error (5xx)
            if error.resp.status in [403, 429, 500, 503]:
                if attempt + 1 < max_retries:
                    wait_time = initial_delay * (backoff_factor ** attempt)
                    print(f"\nAPI error (status: {error.resp.status}). "
                          f"Retrying in {wait_time:.2f} seconds... (Attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    print(f"\nAn error occurred during a batch execution after {max_retries} retries: {error}")
                    raise  # Re-raise the exception if all retries fail
            else:
                # For other HttpErrors, don't retry, just raise
                print(f"\nA non-retriable HTTP error occurred during a batch execution: {error}")
                raise