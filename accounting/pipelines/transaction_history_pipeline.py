import os
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

import accounting.constant as c
from accounting.schemas.transaction_schema import TransactionSchema

class TransactionHistoryPipeline:
    """A pipeline for adding new transactions to the transactions history.

    Attributes:
        file_path (str): The location of the transaction history file
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def extract_transaction_history(self):
        # Add rows from transaction history to current transactions data frame.
        if os.path.exists(self.file_path):
            return pd.read_csv(self.file_path)
        else:
            raise FileExistsError("Transaction history file missing.")

    def clean_transaction_history(self, df):
        # Remove transactions that have already been added.
        df = df.drop_duplicates(subset=[c.DATE, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT, c.SEQUENCE])
        # Sort by date, category, and business.
        df = df.sort_values(by=[c.DATE, c.CATEGORY, c.BUSINESS_OR_PERSON], ascending=[False, True, True])
        return df
    
    @pa.check_types(lazy=True)
    def load_transaction_history(self, df: DataFrame[TransactionSchema], filepath):
        print(df)
        df.to_csv(filepath, index=False)

    @pa.check_types(lazy=True)
    def run_add_to_history_pipeline(self, transactions_to_add_df: DataFrame[TransactionSchema]):
        transaction_history_df = self.extract_transaction_history()
        transaction_history_df = pd.concat([transactions_to_add_df, transaction_history_df], ignore_index=True)
        transaction_history_df = self.clean_transaction_history(transaction_history_df)
        
        self.load_transaction_history(transaction_history_df, c.TRANSACTIONS_HISTORY_FILE_PATH)
