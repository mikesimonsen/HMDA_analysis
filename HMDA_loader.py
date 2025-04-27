import pandas as pd
import sqlite3
import os

# Define the paths
data_file_path = os.path.join('data', '2024_combined_mlar.txt')
database_path = os.path.join('database', 'hmda_analysis.db')

# Ensure the database directory exists
os.makedirs('database', exist_ok=True)

# Step 1: Connect to (or create) the SQLite database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

# Step 2: Drop the table if it already exists
cursor.execute("DROP TABLE IF EXISTS hmda_data;")
print("Dropped existing hmda_data table (if existed).")

# Step 3: Create a new table with the desired schema
create_table_query = """
CREATE TABLE hmda_data (
    activity_year INTEGER,
    lei TEXT,
    loan_type INTEGER,
    loan_purpose INTEGER,
    preapproval INTEGER,
    construction_method INTEGER,
    occupancy_type INTEGER,
    loan_amount NUMERIC,
    action_taken INTEGER,
    state TEXT,
    county TEXT,
    census_tract TEXT,
    ethnicity_applicant_1 TEXT,
    ethnicity_applicant_2 TEXT,
    ethnicity_applicant_3 TEXT,
    ethnicity_applicant_4 TEXT,
    ethnicity_applicant_5 TEXT,
    ethnicity_coapplicant_1 TEXT,
    ethnicity_coapplicant_2 TEXT,
    ethnicity_coapplicant_3 TEXT,
    ethnicity_coapplicant_4 TEXT,
    ethnicity_coapplicant_5 TEXT,
    ethnicity_applicant_collected INTEGER,
    ethnicity_coapplicant_collected INTEGER,
    race_applicant_1 TEXT,
    race_applicant_2 TEXT,
    race_applicant_3 TEXT,
    race_applicant_4 TEXT,
    race_applicant_5 TEXT,
    race_coapplicant_1 TEXT,
    race_coapplicant_2 TEXT,
    race_coapplicant_3 TEXT,
    race_coapplicant_4 TEXT,
    race_coapplicant_5 TEXT,
    race_applicant_collected INTEGER,
    race_coapplicant_collected INTEGER,
    sex_applicant INTEGER,
    sex_coapplicant INTEGER,
    sex_applicant_collected INTEGER,
    sex_coapplicant_collected INTEGER,
    age_applicant TEXT,
    age_applicant_ge62 TEXT,
    age_coapplicant TEXT,
    age_coapplicant_ge62 TEXT,
    income TEXT,
    type_of_purchaser INTEGER,
    rate_spread TEXT,
    hoepa_status INTEGER,
    lien_status INTEGER,
    applicant_credit_model INTEGER,
    coapplicant_credit_model INTEGER,
    reason_for_denial_1 INTEGER,
    reason_for_denial_2 INTEGER,
    reason_for_denial_3 INTEGER,
    reason_for_denial_4 INTEGER,
    total_loan_costs TEXT,
    total_points_and_fees TEXT,
    origination_charges TEXT,
    discount_points TEXT,
    lender_credits TEXT,
    interest_rate TEXT,
    prepayment_penalty_term TEXT,
    debt_to_income_ratio TEXT,
    combined_loan_to_value_ratio TEXT,
    loan_term TEXT,
    introductory_rate_period TEXT,
    balloon_payment INTEGER,
    interest_only_payments INTEGER,
    negative_amortization INTEGER,
    other_non_amortizing_features INTEGER,
    property_value TEXT,
    manufactured_home_secured_property_type INTEGER,
    manufactured_home_land_property_interest INTEGER,
    total_units TEXT,
    multifamily_affordable_units TEXT,
    submission_of_application INTEGER,
    initially_payable_to_institution INTEGER,
    aus_1 INTEGER,
    aus_2 INTEGER,
    aus_3 INTEGER,
    aus_4 INTEGER,
    aus_5 INTEGER,
    reverse_mortgage INTEGER,
    open_end_line_of_credit INTEGER,
    business_or_commercial_purpose INTEGER
);
"""
cursor.execute(create_table_query)
conn.commit()
print("Created new hmda_data table with schema.")

# Step 4: Read the HMDA text file into a pandas DataFrame
try:
    df = pd.read_csv(data_file_path, sep='|', header=None)  # <- Added header=None
    print("Successfully read data file.")
except Exception as e:
    print(f"Error reading the data file: {e}")
    conn.close()
    exit(1)

# Now manually set the column names to match the database schema
df.columns = [
    "activity_year",
    "lei",
    "loan_type",
    "loan_purpose",
    "preapproval",
    "construction_method",
    "occupancy_type",
    "loan_amount",
    "action_taken",
    "state",
    "county",
    "census_tract",
    "ethnicity_applicant_1",
    "ethnicity_applicant_2",
    "ethnicity_applicant_3",
    "ethnicity_applicant_4",
    "ethnicity_applicant_5",
    "ethnicity_coapplicant_1",
    "ethnicity_coapplicant_2",
    "ethnicity_coapplicant_3",
    "ethnicity_coapplicant_4",
    "ethnicity_coapplicant_5",
    "ethnicity_applicant_collected",
    "ethnicity_coapplicant_collected",
    "race_applicant_1",
    "race_applicant_2",
    "race_applicant_3",
    "race_applicant_4",
    "race_applicant_5",
    "race_coapplicant_1",
    "race_coapplicant_2",
    "race_coapplicant_3",
    "race_coapplicant_4",
    "race_coapplicant_5",
    "race_applicant_collected",
    "race_coapplicant_collected",
    "sex_applicant",
    "sex_coapplicant",
    "sex_applicant_collected",
    "sex_coapplicant_collected",
    "age_applicant",
    "age_applicant_ge62",
    "age_coapplicant",
    "age_coapplicant_ge62",
    "income",
    "type_of_purchaser",
    "rate_spread",
    "hoepa_status",
    "lien_status",
    "applicant_credit_model",
    "coapplicant_credit_model",
    "reason_for_denial_1",
    "reason_for_denial_2",
    "reason_for_denial_3",
    "reason_for_denial_4",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
    "interest_rate",
    "prepayment_penalty_term",
    "debt_to_income_ratio",
    "combined_loan_to_value_ratio",
    "loan_term",
    "introductory_rate_period",
    "balloon_payment",
    "interest_only_payments",
    "negative_amortization",
    "other_non_amortizing_features",
    "property_value",
    "manufactured_home_secured_property_type",
    "manufactured_home_land_property_interest",
    "total_units",
    "multifamily_affordable_units",
    "submission_of_application",
    "initially_payable_to_institution",
    "aus_1",
    "aus_2",
    "aus_3",
    "aus_4",
    "aus_5",
    "reverse_mortgage",
    "open_end_line_of_credit",
    "business_or_commercial_purpose"
]


# Step 5: Insert data into the table
try:
    df.to_sql('hmda_data', conn, if_exists='append', index=False)
    print(f"Data successfully imported into {database_path} into table 'hmda_data'.")
except Exception as e:
    print(f"Error inserting data into database: {e}")
finally:
    conn.close()
