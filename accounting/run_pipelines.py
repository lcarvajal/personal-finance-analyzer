from dotenv import load_dotenv
import os

import accounting.constant as c
from accounting.pipelines.cash_transactions_pipeline import CashTransactionsPipeline


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

def main():
    run_cash_transactions_pipeline()

if __name__ == "__main__":
    main()