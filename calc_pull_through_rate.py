import sqlite3
import pandas as pd
import os

# Database and output setup
database_path = os.path.join('database', 'hmda_analysis.db')
output_dir = 'output/pull_through_rate'
os.makedirs(output_dir, exist_ok=True)

# Connect to database
conn = sqlite3.connect(database_path)

# --- Query 1: Pull Through Rate by lender, year, and loan_type ---
query = """
SELECT
    h.activity_year,
    l.respondent_name,
    h.loan_type,
    CASE h.loan_type
        WHEN 1 THEN 'Conventional'
        WHEN 2 THEN 'FHA'
        WHEN 3 THEN 'VA'
        WHEN 4 THEN 'USDA'
        ELSE 'Unknown'
    END AS loan_type_name,
    COUNT(*) AS total_applications,
    SUM(CASE WHEN h.action_taken IN (1, 6) THEN 1 ELSE 0 END) AS closed_loans
FROM
    hmda_data h
LEFT JOIN
    Lender l ON h.lei = l.lei
WHERE
    h.loan_purpose = 1 AND
    h.business_or_commercial_purpose = 2 AND
    h.reverse_mortgage = 2 AND
    h.open_end_line_of_credit = 2 AND
    h.action_taken IN (1, 2, 3, 4, 5, 6, 7, 8)
GROUP BY
    h.activity_year,
    h.loan_type,
    l.respondent_name
ORDER BY
    h.activity_year,
    l.respondent_name,
    h.loan_type;
"""

df = pd.read_sql_query(query, conn)

# Compute Pull Through Rate
df['pull_through_rate'] = (df['closed_loans'] / df['total_applications']).round(4)
df['pull_through_pct'] = (df['pull_through_rate'] * 100).round(2)

# Scaled counts
df['apps_000s'] = (df['total_applications'] / 1000).round(2)
df['closed_000s'] = (df['closed_loans'] / 1000).round(2)

# Save main file
df.to_csv(os.path.join(output_dir, 'pull_through_by_lender.csv'), index=False)
print("✅ Exported: pull_through_by_lender.csv")

# --- Query 2: Totals per lender (all loan types combined) ---
query_totals = """
SELECT
    h.activity_year,
    l.respondent_name,
    COUNT(*) AS total_applications,
    SUM(CASE WHEN h.action_taken IN (1, 6) THEN 1 ELSE 0 END) AS closed_loans
FROM
    hmda_data h
LEFT JOIN
    Lender l ON h.lei = l.lei
WHERE
    h.loan_purpose = 1 AND
    h.business_or_commercial_purpose = 2 AND
    h.reverse_mortgage = 2 AND
    h.open_end_line_of_credit = 2 AND
    h.action_taken IN (1, 2, 3, 4, 5, 6, 7, 8)
GROUP BY
    h.activity_year,
    l.respondent_name
ORDER BY
    h.activity_year,
    l.respondent_name;
"""

df_totals = pd.read_sql_query(query_totals, conn)

df_totals['pull_through_rate'] = (df_totals['closed_loans'] / df_totals['total_applications']).round(4)
df_totals['pull_through_pct'] = (df_totals['pull_through_rate'] * 100).round(2)
df_totals['apps_000s'] = (df_totals['total_applications'] / 1000).round(2)
df_totals['closed_000s'] = (df_totals['closed_loans'] / 1000).round(2)

# Save totals file
df_totals.to_csv(os.path.join(output_dir, 'pull_through_totals_by_lender.csv'), index=False)
print("✅ Exported: pull_through_totals_by_lender.csv")

# Close DB connection
conn.close()
print("✅ Pull Through Rate calculation complete.")
