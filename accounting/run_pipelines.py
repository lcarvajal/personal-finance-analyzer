from dotenv import load_dotenv
import os

import accounting.constant as c
from accounting.pipelines.cash_transactions_pipeline import CashTransactionsPipeline
from accounting.pipelines.credit_card_transactions_pipeline import CreditCardTransactionsPipeline
import accounting.tool as tool


def run_cash_transactions_pipeline(): 
    load_dotenv()

    NOTION_API_KEY = os.getenv(c.NOTION_API_KEY)
    database_id = os.getenv(c.CASH_TRANSACTIONS_DATABASE_ID_KEY)
    url = f'https://api.notion.com/v1/databases/{database_id}/query'

    # Headers including integration token
    headers = {
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28',  # Notion API version
    }

    cash_transactions_pipeline = CashTransactionsPipeline(url=url, headers=headers)
    cash_transactions_pipeline.run_pipeline()

def run_credit_card_transactions_pipeline():
    TEMP_FILES = [f for f in os.listdir(c.TEMP_DIRECTORY_PATH) if os.path.isfile(os.path.join(c.TEMP_DIRECTORY_PATH, f))]
    CSV_FILES = [s for s in TEMP_FILES if s.lower().endswith('csv')]

    credit_card_transactions_pipeline = CreditCardTransactionsPipeline(CSV_FILES)
    credit_card_transactions_pipeline.run_pipeline()

    tool.send_to_trash(CSV_FILES)

def main():
    run_cash_transactions_pipeline()
    run_credit_card_transactions_pipeline()

if __name__ == "__main__":
    main()