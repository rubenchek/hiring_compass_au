from __future__ import annotations

import logging
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
logger = logging.getLogger(__name__)


def authenticate_gmail(
    client_secret_path: Path,
    token_path: Path,
    oauth_host: str = "127.0.0.1",
    oauth_port: int = 0,
    oauth_open_browser: bool = True,
) -> Credentials:
    """
    Authenticate a user against Gmail API and return valid credentials.
    - client_secret_path: OAuth client JSON from Google Cloud
    - token_path: path where token.json will be stored
    """

    creds: Credentials | None = None

    if not client_secret_path.exists():
        raise FileNotFoundError(
            f"Gmail client secret not found: {client_secret_path} "
            "(set HC_GMAIL_CLIENT_SECRET to override)"
        )

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
            if oauth_host == "0.0.0.0" and oauth_port == 0:
                raise ValueError(
                    "oauth_port=0 is not supported when oauth_host=0.0.0.0 (Docker). "
                    "Set HC_GMAIL_OAUTH_PORT=8080 and run docker with -p 8080:8080."
                )
            logger.info(
                "Starting OAuth local server for Gmail: host=%s port=%s open_browser=%s",
                oauth_host,
                oauth_port,
                oauth_open_browser,
            )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path),
                SCOPES,
            )
            creds = flow.run_local_server(
                host=oauth_host, port=oauth_port, open_browser=oauth_open_browser
            )

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
        logger.info("Saved Gmail token to %s", token_path)

    return creds


def build_gmail_service(creds: Credentials):
    logger.info("Building Gmail service client")
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def authenticate_and_build_service(
    client_secret_path: Path,
    token_path: Path,
    oauth_host: str,
    oauth_port: int,
    oauth_open_browser: bool,
):
    logger.info("Authenticating Gmail")
    creds = authenticate_gmail(
        client_secret_path=client_secret_path,
        token_path=token_path,
        oauth_host=oauth_host,
        oauth_port=oauth_port,
        oauth_open_browser=oauth_open_browser,
    )
    return build_gmail_service(creds)
