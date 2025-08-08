# Gmail Productivity Tools

A collection of Python scripts to analyze, label, and archive your Gmail inbox, using the Gmail API and 1Password CLI for secure credential management.

## Features

- **Sender Counter**: Counts emails from each sender and generates a CSV report.
- **Email Labeler**: Automatically finds emails with unsubscribe links and applies nested labels based on the sender's domain (e.g., `Automated/google.com`).
- **Email Archiver**: Downloads emails from a specified date range into a single text file for local analysis or backup.
- **LLM Analyzer**: Uses a Large Language Model (like Google's Gemini) to analyze the content of archived emails.

## Prerequisites

- Python 3.8+
- 1Password CLI installed and configured.
- A Google Cloud Platform project with the Gmail API enabled.
- OAuth 2.0 Client ID credentials downloaded as a JSON file.

## Setup

1.  **Clone the Repository**
    ```bash
    git clone <your-repo-url>
    cd GmailSync
    ```

2.  **Create a Virtual Environment**
    ```bash
    python -m venv venv
    # On Windows, use: venv\Scripts\activate
    # On macOS/Linux, use: source venv/bin/activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Store Google Credentials in 1Password**
    - Take the `credentials.json` file you downloaded from Google Cloud.
    - Create a new "API Credential" item in your 1Password vault.
    - Attach the `credentials.json` file to this item.
    - Get the secret reference for the attached file (e.g., `op://Private/gmail-api-credentials/credentials`). You will use this for the `--creds` argument.

## Usage

Run the scripts from the command line from within the `GmailSync` directory.

**First Run:** The first time you run any script that accesses Gmail, a browser window will open asking you to authorize the application. After you grant access, a `token.json` file will be created, caching your session for future runs. This file is ignored by Git.

### Label Emails
```bash
python gmail-labeler.py --label-unsubscribe "Newsletters" --creds "op://Private/gmail-api-credentials/credentials"
```

### Archive Emails
```bash
python gmail_archiver.py --start-date 2023-01-01 --end-date 2023-12-31 --creds "op://Private/gmail-api-credentials/credentials" --output "archive_2023.txt"
```
---

*This project is for personal use. Be mindful of API rate limits and your data privacy.*