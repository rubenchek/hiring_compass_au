"""
Initialize the local workspace structure (directories, placeholders).

Usage:
    make bootstrap
"""


from hiring_compass_au.workspace import ensure_workspace, format_created_state


def main() -> None:
    state = ensure_workspace()
    print("Workspace ready:")
    print(format_created_state(state))


if __name__ == "__main__":
    main()
