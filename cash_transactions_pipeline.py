import os
import pandas as pd
import constant as c
import requests
from dotenv import load_dotenv

def main():
    load_dotenv()

    # Integration token obtained from Notion
    NOTION_API_KEY = os.getenv('NOTION_API_KEY')

    # Database ID of the table database in Notion
    database_id = os.getenv('CASH_TRANSACTIONS_DATABASE_ID')

    # URL for making API requests
    url = f'https://api.notion.com/v1/databases/{database_id}/query'

    # Headers including integration token
    headers = {
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28',  # Notion API version
    }

    # Make GET request to fetch rows from the database
    response = requests.post(url, headers=headers)

    # Check if request was successful
    if response.status_code == 200:
        # Extract rows from the response JSON
        data = response.json()
        rows = data['results']

        # Initialize empty lists to store extracted data
        person_or_business_list = []
        date_list = []
        category_list = []
        debit_list = []
        credit_list = []

        # Iterate over response data and extract values
        for row in rows:
            properties = row.get('properties', {})
            person_or_business_list.append(properties.get('person_or_business', {}).get('title', [{}])[0].get('plain_text', ''))
            date_list.append(properties.get('date', {}).get('date', {}).get('start', ''))
            category_list.append(properties.get('category', {}).get('select', {}).get('name', ''))
            debit_list.append(properties.get('debit', {}).get('number', None))
            credit_list.append(properties.get('credit', {}).get('number', None))

        # Create DataFrame from extracted data

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
        df = pd.DataFrame(data)

        # Lowercase the values
        df[c.BUSINESS_OR_PERSON_ORIGINAL] = df[c.BUSINESS_OR_PERSON_ORIGINAL].str.lower()
        df[c.BUSINESS_OR_PERSON] = df[c.BUSINESS_OR_PERSON].str.lower()
        df[c.CATEGORY_ORIGINAL] = df[c.CATEGORY_ORIGINAL].str.lower()
        df[c.CATEGORY] = df[c.CATEGORY].str.lower()

        # Replace empty values in 'debit' and 'credit' columns with NaN
        df[c.DEBIT] = df[c.DEBIT].apply(lambda x: float(x) if x is not None else float('nan'))
        df[c.CREDIT] = df[c.CREDIT].apply(lambda x: float(x) if x is not None else float('nan'))

        # Print DataFrame
        print(df)

         # Add rows from transaction history to current transactions data frame.
        if os.path.exists(c.TRANSACTIONS_HISTORY_FILE_PATH):
            transaction_history_df = pd.read_csv(c.TRANSACTIONS_HISTORY_FILE_PATH)
            transactions_df = pd.concat([df, transaction_history_df], ignore_index=True)

        # Remove transactions that have already been added.
        transactions_df = transactions_df.drop_duplicates(subset=[c.DATE, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT, c.SEQUENCE])

        # Sort by date.
        transactions_df = transactions_df.sort_values(by=[c.DATE, c.CATEGORY, c.BUSINESS_OR_PERSON], ascending=[False, True, True])

        # Save for long-term storage.
        transactions_df.to_csv(c.TRANSACTIONS_HISTORY_FILE_PATH, index=False)
    else:
        # Print error message if request failed
        print(f'Error: {response.status_code} - {response.text}')

if __name__ == "__main__":
    main()