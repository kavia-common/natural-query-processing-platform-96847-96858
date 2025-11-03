#!/usr/bin/env python3
"""Initialize and migrate SQLite database for dsp_db.

This script:
- Ensures PRAGMA foreign_keys = ON
- Creates tables app_info and users if they don't exist
- Adds authentication fields to users:
    - password_hash TEXT NOT NULL
    - last_login TIMESTAMP NULL (optional)
- Creates indexes on users(username) and users(email)
- Performs idempotent migrations if the DB file already exists
- Writes db_connection.txt for easy backend consumption
- Writes db_visualizer/sqlite.env for the local DB viewer

Notes for backend consumption:
- The SQLite file path should be provided to the backend via environment variable (e.g., SQLITE_DB).
- Do not hard-code paths in backend code; read from env at runtime.
"""

import sqlite3
import os
import sys
from typing import Optional

DB_NAME = "myapp.db"  # Default DB file for this container
# The backend should read DB path from environment (e.g., SQLITE_DB) rather than hardcoding.


def _enable_foreign_keys(conn: sqlite3.Connection) -> None:
    """Enable foreign keys pragma on the given connection."""
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except sqlite3.Error as e:
        print(f"Warning: failed to enable foreign keys: {e}")


def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    """Check if a table exists."""
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _column_exists(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    """Check if a column exists on a table."""
    try:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        return any(col[1] == column for col in cols)
    except sqlite3.Error as e:
        print(f"Warning: could not inspect columns for {table}: {e}")
        return False


def _index_exists(cursor: sqlite3.Cursor, index_name: str) -> bool:
    """Check if an index exists in the database."""
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (index_name,),
    )
    return cursor.fetchone() is not None


def _create_base_schema(cursor: sqlite3.Cursor) -> None:
    """Create the base schema for new databases."""
    # app_info table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL,
            value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # users table with auth fields (for new DBs include all columns)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            last_login TIMESTAMP NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Ensure indexes exist (safe if table newly created)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"
    )

    # Seed app_info
    cursor.execute(
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("project_name", "dsp_db"),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("version", "0.1.0"),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("author", "John Doe"),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("description", ""),
    )


def _migrate_existing_db(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    """Perform idempotent migrations for existing databases."""
    # Ensure users table exists; if not, create with full schema
    if not _table_exists(cursor, "users"):
        _create_base_schema(cursor)
        return

    # Add password_hash column if missing
    if not _column_exists(cursor, "users", "password_hash"):
        try:
            # Add as NULLABLE first to avoid failures; then update and set NOT NULL if feasible.
            cursor.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
            # For any pre-existing rows, initialize with empty string hash (or placeholder).
            # In production, you might disable login until users reset password.
            cursor.execute("UPDATE users SET password_hash = COALESCE(password_hash, '')")
            print("Added users.password_hash column")
        except sqlite3.Error as e:
            print(f"Warning: could not add password_hash column: {e}")

    # Add last_login column if missing (optional)
    if not _column_exists(cursor, "users", "last_login"):
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN last_login TIMESTAMP")
            print("Added users.last_login column")
        except sqlite3.Error as e:
            print(f"Warning: could not add last_login column: {e}")

    # Create indexes if not exist
    if not _index_exists(cursor, "idx_users_username"):
        try:
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"
            )
        except sqlite3.Error as e:
            print(f"Warning: could not create idx_users_username: {e}")

    if not _index_exists(cursor, "idx_users_email"):
        try:
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"
            )
        except sqlite3.Error as e:
            print(f"Warning: could not create idx_users_email: {e}")

    # Ensure app_info exists
    if not _table_exists(cursor, "app_info"):
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS app_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE NOT NULL,
                    value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        except sqlite3.Error as e:
            print(f"Warning: could not create app_info table: {e}")

    # Seed/refresh app_info keys
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
            ("project_name", "dsp_db"),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
            ("version", "0.1.0"),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
            ("author", "John Doe"),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
            ("description", ""),
        )
    except sqlite3.Error as e:
        print(f"Warning: could not seed app_info: {e}")


def _write_connection_info(db_file: str) -> None:
    """Write db_connection.txt with path and connection string info."""
    current_dir = os.getcwd()
    connection_string = f"sqlite:///{current_dir}/{db_file}"
    try:
        with open("db_connection.txt", "w") as f:
            f.write("# SQLite connection methods:\n")
            f.write(f"# Python: sqlite3.connect('{db_file}')\n")
            f.write(f"# Connection string: {connection_string}\n")
            f.write(f"# File path: {current_dir}/{db_file}\n")
        print("Connection information saved to db_connection.txt")
    except Exception as e:
        print(f"Warning: Could not save connection info: {e}")


def _write_sqlite_env(db_path: str) -> None:
    """Write db_visualizer/sqlite.env for the Node.js DB viewer."""
    if not os.path.exists("db_visualizer"):
        os.makedirs("db_visualizer", exist_ok=True)
        print("Created db_visualizer directory")
    try:
        with open("db_visualizer/sqlite.env", "w") as f:
            f.write(f'export SQLITE_DB="{db_path}"\n')
        print("Environment variables saved to db_visualizer/sqlite.env")
    except Exception as e:
        print(f"Warning: Could not save environment variables: {e}")


def main(db_file: Optional[str] = None) -> int:
    """Entrypoint to initialize or migrate the database."""
    print("Starting SQLite setup...")

    db_file = db_file or DB_NAME
    db_exists = os.path.exists(db_file)

    # Connect and prepare
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return 1

    try:
        cursor = conn.cursor()
        _enable_foreign_keys(conn)

        if db_exists:
            print(f"SQLite database already exists at {db_file}")
            # Sanity check
            try:
                conn.execute("SELECT 1")
                print("Database is accessible and working.")
            except Exception as e:
                print(f"Warning: Database exists but may be corrupted: {e}")

            # Perform migrations
            _migrate_existing_db(conn, cursor)
        else:
            print("Creating new SQLite database...")
            _create_base_schema(cursor)

        conn.commit()

        # Gather basic stats
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            table_count = cursor.fetchone()[0]
        except sqlite3.Error:
            table_count = 0

        try:
            cursor.execute("SELECT COUNT(*) FROM app_info")
            record_count = cursor.fetchone()[0]
        except sqlite3.Error:
            record_count = 0

    finally:
        conn.close()

    # Persist connection info and viewer env file
    _write_connection_info(db_file)
    db_path_abs = os.path.abspath(db_file)
    _write_sqlite_env(db_path_abs)

    # Log helpful usage info
    current_dir = os.getcwd()
    connection_string = f"sqlite:///{current_dir}/{db_file}"
    print("\nSQLite setup complete!")
    print(f"Database: {db_file}")
    print(f"Location: {current_dir}/{db_file}\n")
    print("To use with Node.js viewer, run: source db_visualizer/sqlite.env\n")
    print("To connect to the database, use one of the following methods:")
    print(f"1. Python: sqlite3.connect('{db_file}')")
    print(f"2. Connection string: {connection_string}")
    print(f"3. Direct file access: {current_dir}/{db_file}\n")
    print("Database statistics:")
    print(f"  Tables: {table_count}")
    print(f"  App info records: {record_count}")

    # If sqlite3 CLI is available, show how to use it
    try:
        import subprocess

        result = subprocess.run(["which", "sqlite3"], capture_output=True, text=True)
        if result.returncode == 0:
            print("")
            print("SQLite CLI is available. You can also use:")
            print(f"  sqlite3 {db_file}")
    except Exception:
        pass

    print("\nScript completed successfully.")
    return 0


if __name__ == "__main__":
    # Optional: allow overriding DB file via env if set (e.g., for local runs)
    db_file_override = os.environ.get("SQLITE_DB") or None
    sys.exit(main(db_file_override))
