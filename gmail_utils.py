import os.path
import subprocess
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