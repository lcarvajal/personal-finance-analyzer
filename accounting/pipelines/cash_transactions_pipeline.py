import pandas as pd
import requests

import accounting.constant as c
from accounting.pipelines.transaction_history_pipeline import TransactionHistoryPipeline

class CashTransactionsPipeline:
    """A pipeline that extracts cash transactions stored on Notion and loads them into transaction history.

    Attributes:
        url (str): The url for the Notion API.
        headers ({str:str}) The headers for the Notion API request.
    """

    def __init__(self, url, headers):
        self.url = url
        self.headers = headers
    
    def extract_transactions(self):
        response = requests.post(self.url, headers=self.headers)

        if response.status_code == 200:
            data = response.json()
            rows = data['results']

            person_or_business_list = []
            date_list = []
            category_list = []
            debit_list = []
            credit_list = []

            # Iterate over response data and extract values into data frame
            for row in rows:
                properties = row.get('properties', {})
                person_or_business_list.append(properties.get('person_or_business', {}).get('title', [{}])[0].get('plain_text', ''))
                date_list.append(properties.get('date', {}).get('date', {}).get('start', ''))
                category_list.append(properties.get('category', {}).get('select', {}).get('name', ''))
                debit_list.append(properties.get('debit', {}).get('number', None))
                credit_list.append(properties.get('credit', {}).get('number', None))

            data = {
                c.DATE: date_list,
                c.CATEGORY_ORIGINAL: category_list,
                c.CATEGORY: category_list,
                c.DEBIT: debit_list,
                c.CREDIT: credit_list,
                c.SEQUENCE: 1,
                c.BUSINESS_OR_PERSON_ORIGINAL: person_or_business_list,
                c.BUSINESS_OR_PERSON: person_or_business_list
            }

            return pd.DataFrame(data)
        else:
            print(f'Error: {response.status_code} - {response.text}')
            raise BrokenPipeError("Error extracting cash transactions from Notion.")
    
    def clean_transactions(self, df):
         # Lowercase values
        df[c.BUSINESS_OR_PERSON_ORIGINAL] = df[c.BUSINESS_OR_PERSON_ORIGINAL].str.lower()
        df[c.BUSINESS_OR_PERSON] = df[c.BUSINESS_OR_PERSON].str.lower()
        df[c.CATEGORY_ORIGINAL] = df[c.CATEGORY_ORIGINAL].str.lower()
        df[c.CATEGORY] = df[c.CATEGORY].str.lower()

        # Replace empty values in 'debit' and 'credit' columns with NaN
        df[c.DEBIT] = df[c.DEBIT].apply(lambda x: float(x) if x is not None else float('nan'))
        df[c.CREDIT] = df[c.CREDIT].apply(lambda x: float(x) if x is not None else float('nan'))

        return df
    
    def load_transactions_to_transaction_history(self, df):
        transaction_history_pipeline = TransactionHistoryPipeline(file_path=c.TRANSACTIONS_HISTORY_FILE_PATH)
        transaction_history_pipeline.run_add_to_history_pipeline(transactions_to_add_df=df)         

    def run_pipeline(self):
        df = self.extract_transactions()
        df = self.clean_transactions(df)
        self.load_transactions_to_transaction_history(df)
