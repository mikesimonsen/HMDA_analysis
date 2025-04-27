import sqlite3
import pandas as pd
import os

# Define paths
database_path = os.path.join('database', 'hmda_analysis.db')
panel_file_path = os.path.join('data', '2023_ts.txt')

# Ensure database directory exists
os.makedirs('database', exist_ok=True)

# Step 1: Connect to (or create) the SQLite database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

# Step 2: Drop the existing Lender table if it exists
cursor.execute("DROP TABLE IF EXISTS Lender;")
print("Dropped existing Lender table (if existed).")

# Step 3: Create a new Lender table with corrected columns
create_table_query = """
CREATE TABLE Lender (
    activity_year INTEGER,
    calendar_quarter INTEGER,
    lei TEXT PRIMARY KEY,
    tax_id TEXT,
    agency_code INTEGER,
    respondent_name TEXT,
    respondent_state TEXT,
    respondent_city TEXT,
    respondent_zip_code TEXT,
    lar_count INTEGER
);
"""
cursor.execute(create_table_query)
conn.commit()
print("Created new Lender table.")

# Step 4: Load the 2023 Transmittal Sheet
try:
    panel = pd.read_csv(panel_file_path, sep='|')
    print(f"Loaded panel file with {len(panel):,} records.")
except Exception as e:
    print(f"Error reading panel file: {e}")
    conn.close()
    exit(1)

# Step 5: Insert the panel data into the Lender table
try:
    # No need to rename columns, they match perfectly
    panel.to_sql('Lender', conn, if_exists='append', index=False)
    print("Lender data successfully imported into the database.")
except Exception as e:
    print(f"Error inserting lender data into database: {e}")
finally:
    conn.close()
