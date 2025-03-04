#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/3/3 18:36
# @Author  ideal93
# @File  migrate.py
# @Brief

import psycopg2


def migrate(db_config, migration_file, params=None):
    """
    Applies a migration to a PostgreSQL database.

    Args:
        db_config (dict): Dictionary containing database configuration parameters.
        migration_file (str): Path to the SQL file containing the migration.
        params (dict or tuple, optional): Parameters to inject into the SQL query.
    """
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Read the migration SQL from file
        with open(migration_file, "r") as file:
            migration_sql = file.read()

        if params:
            cursor.execute(migration_sql, params)
        else:
            cursor.execute(migration_sql)

        conn.commit()
        print("Migration applied successfully.")

    except Exception as e:
        print("Migration failed:", e)
    finally:
        if conn:
            conn.close()
