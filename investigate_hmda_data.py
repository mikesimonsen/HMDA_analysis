import sqlite3
import pandas as pd
import os

# Define database path
database_path = os.path.join('database', 'hmda_analysis.db')

# Ensure output directory exists
os.makedirs('output', exist_ok=True)

# Step 1: Connect to the SQLite database
conn = sqlite3.connect(database_path)

# Define common WHERE clause for filtering
where_clause = """
WHERE
    h.business_or_commercial_purpose = 2
    AND h.open_end_line_of_credit = 2
    AND h.reverse_mortgage = 2
    AND h.loan_purpose = 1
"""

# ---- Analysis 1: Loans by Year, Loan Type, and Lender Name ----
query_year_loan_type = f"""
SELECT
    h.activity_year,
    h.loan_type,
    CASE h.loan_type
        WHEN 1 THEN 'Conventional'
        WHEN 2 THEN 'FHA'
        WHEN 3 THEN 'VA'
        WHEN 4 THEN 'USDA'
        ELSE 'Unknown'
    END AS loan_type_name,
    l.respondent_name,
    COUNT(*) AS loan_count,
    SUM(h.loan_amount) AS total_loan_amount,
    AVG(h.loan_amount) AS avg_loan_amount
FROM
    hmda_data h
LEFT JOIN
    Lender l
ON
    h.lei = l.lei
{where_clause}
GROUP BY
    h.activity_year,
    h.loan_type,
    l.respondent_name
ORDER BY
    h.activity_year,
    h.loan_type,
    loan_count DESC;
"""

df_year_loan_type = pd.read_sql_query(query_year_loan_type, conn)
print("\nFiltered Loan Counts, Totals, and Averages by Year, Loan Type, and Lender Name:")
print(df_year_loan_type.head(20))

# Export to CSV
df_year_loan_type.to_csv('output/loans_by_year_loan_type_lender.csv', index=False)


# ---- Analysis 2: Loans by Year, State, and Lender Name ----
query_year_state = f"""
SELECT
    h.activity_year,
    h.state,
    l.respondent_name,
    COUNT(*) AS loan_count,
    SUM(h.loan_amount) AS total_loan_amount,
    AVG(h.loan_amount) AS avg_loan_amount
FROM
    hmda_data h
LEFT JOIN
    Lender l
ON
    h.lei = l.lei
{where_clause}
GROUP BY
    h.activity_year,
    h.state,
    l.respondent_name
ORDER BY
    h.activity_year,
    h.state,
    loan_count DESC;
"""

df_year_state = pd.read_sql_query(query_year_state, conn)
print("\nFiltered Loan Counts, Totals, and Averages by Year, State, and Lender Name:")
print(df_year_state.head(20))

# Export to CSV
df_year_state.to_csv('output/loans_by_year_state_lender.csv', index=False)


# ---- Analysis 3: Loans by Year, Loan Type, and Lender Name (for lender rollups) ----
query_year_lender = f"""
SELECT
    h.activity_year,
    h.loan_type,
    CASE h.loan_type
        WHEN 1 THEN 'Conventional'
        WHEN 2 THEN 'FHA'
        WHEN 3 THEN 'VA'
        WHEN 4 THEN 'USDA'
        ELSE 'Unknown'
    END AS loan_type_name,
    l.respondent_name,
    COUNT(*) AS loan_count,
    SUM(h.loan_amount) AS total_loan_amount,
    AVG(h.loan_amount) AS avg_loan_amount
FROM
    hmda_data h
LEFT JOIN
    Lender l
ON
    h.lei = l.lei
{where_clause}
GROUP BY
    h.activity_year,
    h.loan_type,
    l.respondent_name
ORDER BY
    h.activity_year,
    h.loan_type,
    loan_count DESC;
"""

df_year_lender = pd.read_sql_query(query_year_lender, conn)
print("\nFiltered Loan Counts, Totals, and Averages by Year, Loan Type, and Lender Name (Lender Summary):")
print(df_year_lender.head(20))

# ---- Bonus Pivot Tables ----

# Pivot for Loan Counts
pivot_counts = df_year_lender.pivot_table(
    index='respondent_name',
    columns='loan_type_name',
    values='loan_count',
    aggfunc='sum',
    fill_value=0
)

pivot_counts = pivot_counts.reset_index()
print("\nPivot table: Loan Counts by Lender and Loan Type:")
print(pivot_counts.head(10))

pivot_counts.to_csv('output/loan_counts_pivot_by_lender.csv', index=False)

# Pivot for Total Loan Amounts
pivot_amounts = df_year_lender.pivot_table(
    index='respondent_name',
    columns='loan_type_name',
    values='total_loan_amount',
    aggfunc='sum',
    fill_value=0
)

pivot_amounts = pivot_amounts.reset_index()
print("\nPivot table: Total Loan Amounts by Lender and Loan Type:")
print(pivot_amounts.head(10))




# Export to CSV
df_year_lender.to_csv('output/loans_by_year_lender.csv', index=False)
pivot_amounts.to_csv('output/loan_amounts_pivot_by_lender.csv', index=False)

# Step 2: Close connection
conn.close()

print("\n✅ All investigation results exported to 'output/' folder, now including human-readable loan types.")
