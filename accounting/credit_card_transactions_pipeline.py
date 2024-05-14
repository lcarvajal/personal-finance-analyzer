'''
This program takes temporary credit card transaction data, cleans it up, and adds it to a CSV file containing a history of all credit card transactions.
'''

from datetime import datetime
from dotenv import load_dotenv
import os
from openai import OpenAI
import pandas as pd
import send2trash
from transaction_category import categorize_transactions, check_for_approved_categories, get_category_from_api
from transaction_history_pipeline import load_transaction_history
import constant as c

load_dotenv()
OPENAI_API_KEY = os.getenv(c.OPEN_AI_KEY)

TEMP_FILES = [f for f in os.listdir(c.TEMP_DIRECTORY_PATH) if os.path.isfile(os.path.join(c.TEMP_DIRECTORY_PATH, f))]
CSV_FILES = [s for s in TEMP_FILES if s.lower().endswith('csv')] 

# Extract

def extract_capital_one_transactions(csv):
    """Extracts a Capital One CSV file into a dataframe."""
    df = pd.read_csv(csv, encoding='latin-1')
    
    df = df.rename(columns={
        c.CAP_ONE_TRANSACTION_DATE: c.DATE, 
        c.CAP_ONE_CARD_NUMBER: c.CARD_NUMBER, 
        c.CAP_ONE_DESCRIPTION: c.BUSINESS_OR_PERSON_ORIGINAL, 
        c.CAP_ONE_CATEGORY: c.CATEGORY, 
        c.CAP_ONE_DEBIT: c.DEBIT, 
        c.CAP_ONE_CREDIT: c.CREDIT })
    
    # Check dataframe is in expected format.
    expected_columns = [c.DATE, c.CARD_NUMBER, c.BUSINESS_OR_PERSON_ORIGINAL, c.CATEGORY, c.DEBIT, c.CREDIT]
    if not all(col in df.columns for col in expected_columns):
        raise TypeError(f"{csv} contains one or more unexpected column names.")

    return df

# Transform

def clean_capital_one_transactions(df):
    """Drop data not needed for transaction analysis and reformat business names to keep naming consistent."""
    df[c.BUSINESS_OR_PERSON_ORIGINAL] = df[c.BUSINESS_OR_PERSON_ORIGINAL].str.lower()
    df[c.BUSINESS_OR_PERSON] = df[c.BUSINESS_OR_PERSON_ORIGINAL].str.replace('[\d#]+', '', regex=True)
    df = df.dropna(subset=[c.DEBIT])
    df.drop('Posted Date', axis=1, inplace=True)
    return df

def set_unique_identifiers(df):
    """Create a unique identifier to avoid readding existing transactions to transaction history."""
    df[c.SEQUENCE] = df.groupby([c.DATE, c.CARD_NUMBER, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT]).cumcount() + 1
    return df

# Load

def load_transactions(df):
    """Store imported transactions in CSVs as backups."""
    # Get today's date
    today_date = datetime.today().strftime('%Y-%m-%d')

    # Create filename with today's date
    filename = f"transactions_{today_date}.csv"
    df.to_csv(c.IMPORTED_TRANSACTIONS_DIRECTORY_PATH + filename, index=False)

def send_csv_files_to_trash():
    """Moves CSV files in temp directory to trash."""
    for file in CSV_FILES:
        file_path = c.TEMP_DIRECTORY_PATH + file

        # Check if the file exists before attempting to delete
        if os.path.exists(file_path):
            # Move the file to the trash
            send2trash.send2trash(file_path)
            print(f"File '{file_path}' moved to trash successfully.")
        else:
            print(f"File '{file_path}' does not exist.")

# Main

def main():
    # Extract data by looping through CSVs in temp and add them all to one dataframe
    transactions_df = pd.DataFrame()

    for file in CSV_FILES:
        df = extract_capital_one_transactions(c.TEMP_DIRECTORY_PATH + file)
        df = clean_capital_one_transactions(df)
        df = categorize_transactions(df)
        df = set_unique_identifiers(df)
        df[c.CATEGORY] = df.apply(get_category_from_api, axis=1)
        transactions_df = pd.concat([transactions_df, df])

    if transactions_df.empty:
        return
    else:
        load_transactions(transactions_df)
        load_transaction_history(transactions_df)

        check_for_approved_categories(transactions_df)
        send_csv_files_to_trash()

if __name__ == "__main__":
    main()