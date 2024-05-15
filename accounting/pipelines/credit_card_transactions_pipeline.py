'''
This pipeline takes temporary credit card transaction data, cleans it up, and loads it to a CSV file containing a history of all credit card transactions.
'''

from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

import accounting.constant as c
import accounting.tool as tool
from accounting.transaction_category import categorize_transactions, get_category_from_api
from accounting.pipelines.transaction_history_pipeline import TransactionHistoryPipeline
from accounting.schemas.transaction_schema import TransactionSchema, CapitalOneTransactionSchema

load_dotenv()
OPENAI_API_KEY = os.getenv(c.OPEN_AI_KEY)

TEMP_FILES = [f for f in os.listdir(c.TEMP_DIRECTORY_PATH) if os.path.isfile(os.path.join(c.TEMP_DIRECTORY_PATH, f))]
CSV_FILES = [s for s in TEMP_FILES if s.lower().endswith('csv')]

# Extract

def extract_capital_one_transactions(csv):
    """Extracts a Capital One CSV file into a dataframe."""
    df = pd.read_csv(csv, encoding='latin-1')
    return df

# Transform

@pa.check_types(lazy=True)
def transform_capital_one_transactions(df: DataFrame[CapitalOneTransactionSchema]):
    """Drop data not needed for transaction analysis and reformat business names to keep naming consistent."""

    df = df.rename(columns={
        c.CAP_ONE_TRANSACTION_DATE: c.DATE, 
        c.CAP_ONE_CARD_NUMBER: c.CARD_NUMBER, 
        c.CAP_ONE_DESCRIPTION: c.BUSINESS_OR_PERSON_ORIGINAL, 
        c.CAP_ONE_CATEGORY: c.CATEGORY, 
        c.CAP_ONE_DEBIT: c.DEBIT, 
        c.CAP_ONE_CREDIT: c.CREDIT })
    
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

@pa.check_types(lazy=True)
def load_transactions(df: DataFrame[TransactionSchema], filepath: str):
    """Store imported transactions in CSVs as backups."""
    df.to_csv(filepath, index=False)

# Main

def main():
    # Extract data by looping through CSVs in temp and add them all to one dataframe
    transactions_df = pd.DataFrame()

    for file in CSV_FILES:
        df = extract_capital_one_transactions(c.TEMP_DIRECTORY_PATH + file)
        df = transform_capital_one_transactions(df)
        df = categorize_transactions(df)
        df = set_unique_identifiers(df)
        df[c.CATEGORY] = df.apply(get_category_from_api, axis=1)
        transactions_df = pd.concat([transactions_df, df])

    if transactions_df.empty:
        return
    else:
        # Create filename with today's date
        today_date = datetime.today().strftime('%Y-%m-%d')
        todays_transactions_filename = f"transactions_{today_date}.csv"
        todays_transactions_filepath = c.IMPORTED_TRANSACTIONS_DIRECTORY_PATH + todays_transactions_filename

        load_transactions(transactions_df, todays_transactions_filepath)

        transaction_history_pipeline = TransactionHistoryPipeline(file_path=c.TRANSACTIONS_HISTORY_FILE_PATH)
        transaction_history_pipeline.run_add_to_history_pipeline(transactions_to_add_df=transactions_df)

        tool.send_to_trash(CSV_FILES)

if __name__ == "__main__":
    main()