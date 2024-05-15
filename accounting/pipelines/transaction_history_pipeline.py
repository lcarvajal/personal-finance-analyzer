import os
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

import accounting.constant as c
from accounting.schemas.transaction_schema import TransactionSchema


def extract_transaction_history():
    # Add rows from transaction history to current transactions data frame.
    if os.path.exists(c.TRANSACTIONS_HISTORY_FILE_PATH):
        return pd.read_csv(c.TRANSACTIONS_HISTORY_FILE_PATH)
    else:
        raise FileExistsError("Transaction history file missing.")
    
def clean_transaction_history(df):
    # Remove transactions that have already been added.
    df = df.drop_duplicates(subset=[c.DATE, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT, c.SEQUENCE])
    # Sort by date.
    df = df.sort_values(by=[c.DATE, c.CATEGORY, c.BUSINESS_OR_PERSON], ascending=[False, True, True])
    return df

@pa.check_types(lazy=True)
def load_transaction_history(df: DataFrame[TransactionSchema]):
    transaction_history_df = extract_transaction_history()
    transaction_history_df = pd.concat([df, transaction_history_df], ignore_index=True)
    transaction_history_df = clean_transaction_history(transaction_history_df)
    # Save for long-term storage.
    transaction_history_df.to_csv(c.TRANSACTIONS_HISTORY_FILE_PATH, index=False)
