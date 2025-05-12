import sqlite3
import pandas as pd
import os

# Define database path
database_path = os.path.join('database', 'hmda_analysis.db')

# Ensure output directory exists
os.makedirs('output', exist_ok=True)

# Prompt: Analyze only closed loans?
user_input = input("Analyze only closed loans? (Y/N) [default: Y]: ").strip().upper()
if user_input == '':
    user_input = 'Y'

if user_input == 'Y':
    closed_filter = "AND h.action_taken IN (1, 6)"
    print("✅ Restricting analysis to closed loans (action_taken = 1 or 6).")
else:
    closed_filter = ""
    print("✅ Including all loan applications and statuses.")

# Prompt: Loan type selection
print("\nLoan type filter options:")
print("  Total - Sum across all loan types per lender")
print("  All   - Analyze all loan types (grouped)")
print("  1     - Conventional")
print("  2     - FHA")
print("  3     - VA")
print("  4     - USDA")

loan_type_input = input("Which loan type? [default: All]: ").strip().upper()
if loan_type_input == "":
    loan_type_input = "ALL"

if loan_type_input == "TOTAL":
    loan_type_filter = ""
    loan_type_group = ""
    loan_type_select = ""
    filename_suffix = "_total"
    print("✅ Summing across all loan types.")
elif loan_type_input == "ALL":
    loan_type_filter = ""
    loan_type_group = ", h.loan_type, loan_type_name"
    loan_type_select = """
    h.loan_type,
    CASE h.loan_type
        WHEN 1 THEN 'Conventional'
        WHEN 2 THEN 'FHA'
        WHEN 3 THEN 'VA'
        WHEN 4 THEN 'USDA'
        ELSE 'Unknown'
    END AS loan_type_name,
    """
    filename_suffix = ""
    print("✅ Including all loan types (split in output).")
elif loan_type_input in {"1", "2", "3", "4"}:
    loan_type_filter = f"AND h.loan_type = {loan_type_input}"
    loan_type_group = ", h.loan_type, loan_type_name"
    loan_type_select = f"""
    h.loan_type,
    CASE h.loan_type
        WHEN 1 THEN 'Conventional'
        WHEN 2 THEN 'FHA'
        WHEN 3 THEN 'VA'
        WHEN 4 THEN 'USDA'
        ELSE 'Unknown'
    END AS loan_type_name,
    """
    filename_suffix = f"_loan_type_{loan_type_input}"
    print(f"✅ Filtering to loan_type = {loan_type_input}.")
else:
    print("❌ Invalid loan type selection. Exiting.")
    exit(1)

# WHERE clause base
where_clause = f"""
WHERE
    h.business_or_commercial_purpose = 2
    AND h.open_end_line_of_credit = 2
    AND h.reverse_mortgage = 2
    AND h.loan_purpose = 1
    {closed_filter}
    {loan_type_filter}
"""

# Step 1: Connect to the SQLite database
conn = sqlite3.connect(database_path)

# ---- View 1: Loans by Year, Loan Type, and Lender Name ----
query_year_loan_type = f"""
SELECT
    h.activity_year,
    {loan_type_select}
    l.respondent_name,
    COUNT(*) AS loan_count,
    SUM(h.loan_amount) AS total_loan_amount,
    AVG(h.loan_amount) AS avg_loan_amount
FROM
    hmda_data h
LEFT JOIN
    Lender l ON h.lei = l.lei
{where_clause}
GROUP BY
    h.activity_year,
    l.respondent_name
    {loan_type_group}
ORDER BY
    h.activity_year,
    loan_count DESC;
"""
df_year_loan_type = pd.read_sql_query(query_year_loan_type, conn)
df_year_loan_type["loan_count_000s"] = (df_year_loan_type["loan_count"] / 1000).round(2)
df_year_loan_type["loan_amount_bil"] = (df_year_loan_type["total_loan_amount"] / 1_000_000_000).round(3)
df_year_loan_type.to_csv(f'output/loans_by_year_lender{filename_suffix}.csv', index=False)
print(f"✅ Exported: loans_by_year_lender{filename_suffix}.csv")

# ---- View 2: Loans by Year, State, and Lender Name ----
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
    Lender l ON h.lei = l.lei
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
df_year_state["loan_count_000s"] = (df_year_state["loan_count"] / 1000).round(2)
df_year_state["loan_amount_bil"] = (df_year_state["total_loan_amount"] / 1_000_000_000).round(3)
df_year_state.to_csv(f'output/loans_by_year_state_lender{filename_suffix}.csv', index=False)
print(f"✅ Exported: loans_by_year_state_lender{filename_suffix}.csv")

# ---- View 3: Loans by Year, Loan Type, and Lender Name (for Pivoting) ----
df_year_lender = df_year_loan_type.copy()

# ---- Optional Pivot Tables ----
if loan_type_input in {"ALL", "1", "2", "3", "4"} and "loan_type_name" in df_year_lender.columns:
    pivot_counts = df_year_lender.pivot_table(
        index='respondent_name',
        columns='loan_type_name',
        values='loan_count',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    pivot_counts.to_csv(f'output/loan_counts_pivot_by_lender{filename_suffix}.csv', index=False)
    print(f"✅ Exported pivot: loan_counts_pivot_by_lender{filename_suffix}.csv")

    pivot_amounts = df_year_lender.pivot_table(
        index='respondent_name',
        columns='loan_type_name',
        values='total_loan_amount',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    pivot_amounts.to_csv(f'output/loan_amounts_pivot_by_lender{filename_suffix}.csv', index=False)
    print(f"✅ Exported pivot: loan_amounts_pivot_by_lender{filename_suffix}.csv")

# Close DB connection
conn.close()
print("\n✅ Investigation complete.")
