from __future__ import annotations

from pathlib import Path
import logging

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
logger = logging.getLogger(__name__)

def authenticate_gmail(client_secret_path: Path, token_path: Path) -> Credentials:
    """
    Authenticate a user against Gmail API and return valid credentials.
    - client_secret_path: OAuth client JSON from Google Cloud
    - token_path: path where token.json will be stored
    """
        
    creds: Credentials | None = None
    
    # Load existing token if present
    if token_path.exists():
        logger.info("Loading existing Gmail token from %s", token_path)
        creds = Credentials.from_authorized_user_file(
        str(token_path),
        SCOPES,
        )
        
    # If no valid credentials, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing expired Gmail token")
            creds.refresh(Request())
        else:
            logger.info("Starting OAuth flow (no valid token found)")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path),
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
        logger.info("Saved Gmail token to %s", token_path)

    return creds

def build_gmail_service(creds: Credentials):
    logger.info("Building Gmail service client")
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def authenticate_and_build_service(client_secret_path : Path, token_path: Path):
    logger.info("Authenticating Gmail")
    creds = authenticate_gmail(
        client_secret_path=client_secret_path,
        token_path=token_path
        )
    return build_gmail_service(creds)