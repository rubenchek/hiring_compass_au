from pathlib import Path
from hiring_compass_au.workspace import get_workspace
from hiring_compass_au.storage.mail_store import get_connection, init_all_tables

def main():
    ws = get_workspace()
    conn = get_connection(ws.db_path)
    init_all_tables(conn)
    conn.close()

if __name__ == "__main__":
    main()