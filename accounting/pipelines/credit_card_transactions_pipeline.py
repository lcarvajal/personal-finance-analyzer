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

class CreditCardTransactionsPipeline:
    """A pipeline that loops through file paths and etls transaction data.

    Attributes:
        file_paths ([str]): The file paths containing all the transaction data,
        transactions_df (DataFrame): The cleaned up data frame loaded at the end of the pipeline.
    """

    def __init__(self, transaction_file_paths):
        self.file_paths = transaction_file_paths
        self.transactions_df = pd.DataFrame()

    # Extract
    def extract_capital_one_transactions(self, file_path):
        """Extracts a Capital One CSV file into a dataframe."""
        df = pd.read_csv(file_path, encoding='latin-1')
        return df

    # Transform

    @pa.check_types(lazy=True)
    def clean_capital_one_transactions(self, df: DataFrame[CapitalOneTransactionSchema]):
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

    def set_unique_identifiers(self, df):
        """Create a unique identifier to avoid readding existing transactions to transaction history."""
        df[c.SEQUENCE] = df.groupby([c.DATE, c.CARD_NUMBER, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT]).cumcount() + 1
        return df

    # Load

    @pa.check_types(lazy=True)
    def load_transactions(self, df: DataFrame[TransactionSchema], filepath: str):
        """Store imported transactions in CSVs as backups."""
        df.to_csv(filepath, index=False)

    # Pipeline

    def run_pipeline(self):
        # Extract data by looping through CSVs in temp and add them all to one dataframe
        transactions_df = pd.DataFrame()

        for file in self.file_paths:
            df = self.extract_capital_one_transactions(c.TEMP_DIRECTORY_PATH + file)
            df = self.clean_capital_one_transactions(df)
            df = categorize_transactions(df)
            df = self.set_unique_identifiers(df)
            df[c.CATEGORY] = df.apply(get_category_from_api, axis=1)
            transactions_df = pd.concat([transactions_df, df])

        if transactions_df.empty:
            print(f"No new transactions found in {CSV_FILES}.")
        else :
            # Create filename with today's date
            today_date = datetime.today().strftime('%Y-%m-%d')
            todays_transactions_filename = f"transactions_{today_date}.csv"
            todays_transactions_filepath = c.IMPORTED_TRANSACTIONS_DIRECTORY_PATH + todays_transactions_filename

            self.transactions_df = transactions_df
            self.load_transactions(transactions_df, todays_transactions_filepath)

# Main

def main():
    credit_card_transactions_pipeline = CreditCardTransactionsPipeline(CSV_FILES)
    credit_card_transactions_pipeline.run_pipeline()

    new_transactions_df = credit_card_transactions_pipeline.transactions_df
    if new_transactions_df.empty:
        return
    else:
        transaction_history_pipeline = TransactionHistoryPipeline(file_path=c.TRANSACTIONS_HISTORY_FILE_PATH)
        transaction_history_pipeline.run_add_to_history_pipeline(transactions_to_add_df=new_transactions_df)

        tool.send_to_trash(CSV_FILES)

if __name__ == "__main__":
    main()