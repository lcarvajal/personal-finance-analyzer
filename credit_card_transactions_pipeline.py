'''
This program takes temporary credit card transaction data, cleans it up, and adds it to a CSV file containing a history of all credit card transactions.
'''

from datetime import datetime
from dotenv import load_dotenv
import os
from openai import OpenAI
import pandas as pd
import send2trash
import constant as c

load_dotenv()
OPENAI_API_KEY = os.getenv(c.OPEN_AI_KEY)

TEMP_FILES = [f for f in os.listdir(c.TEMP_DIRECTORY_PATH) if os.path.isfile(os.path.join(c.TEMP_DIRECTORY_PATH, f))]
CSV_FILES = [s for s in TEMP_FILES if s.lower().endswith('csv')] 

def get_categories_df():
    df = pd.read_csv(c.DATA_DIRECTORY_PATH + 'categories.csv')
    column_to_drop = 'Unnamed: 1'
    if column_to_drop in df.columns:
        df.drop(column_to_drop, axis=1, inplace=True)
    pd.set_option('display.max_rows', None)
    print("Valid Categories:")
    print(df)
    pd.set_option('display.max_rows', 60)
    return df

CATEGORIES_DF = get_categories_df()

# Extract

def extract_capital_one_transactions(csv):
    """Reads a Capital One CSV file and preprocesses it."""
    df = pd.read_csv(csv, encoding='latin-1')
    
    df = df.rename(columns={
        'Transaction Date': c.DATE, 
        'Card No.': c.CARD_NUMBER, 
        'Description': c.BUSINESS_OR_PERSON_ORIGINAL, 
        'Category': c.CATEGORY, 
        'Debit': c.DEBIT, 
        'Credit': c.CREDIT })
    
    # Check dataframe is in expected format.
    expected_columns = [c.DATE, c.CARD_NUMBER, c.BUSINESS_OR_PERSON_ORIGINAL, c.CATEGORY, c.DEBIT, c.CREDIT]
    if not all(col in df.columns for col in expected_columns):
        raise ValueError("DataFrame is missing one or more expected columns.")

    return df

# Transform

def clean_capital_one_transactions(df):
    df[c.BUSINESS_OR_PERSON_ORIGINAL] = df[c.BUSINESS_OR_PERSON_ORIGINAL].str.lower()
    df[c.BUSINESS_OR_PERSON] = df[c.BUSINESS_OR_PERSON_ORIGINAL].str.replace('[\d#]+', '', regex=True)
    df = df.dropna(subset=[c.DEBIT])
    df.drop('Posted Date', axis=1, inplace=True)
    return df

def categorize_transactions(df):
    df[c.CATEGORY] = df[c.CATEGORY].str.lower()

    # Merge transactions with categorized_businesses_df to get correct categories
    categorized_businesses_df = pd.read_csv(c.DATA_DIRECTORY_PATH + 'categorized_businesses.csv')
    df = pd.merge(df, categorized_businesses_df, on=c.BUSINESS_OR_PERSON, how='left')

    df = df.rename(columns={
        'category_x': c.CATEGORY_ORIGINAL, 
        'category_y': c.CATEGORY})
    
    # Merge with categories to get the correct category names
    df = pd.merge(df, CATEGORIES_DF, on=c.CATEGORY, how='left')
    
    return df

def set_unique_identifiers(df):
    # Create a unique identifier for each transaction.
    df[c.SEQUENCE] = df.groupby([c.DATE, c.CARD_NUMBER, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT]).cumcount() + 1
    return df

def get_valid_category():
    while True:
        user_input = input("Enter a category: ").strip().lower()

        if user_input in CATEGORIES_DF[c.CATEGORY].str.lower().values:
            return user_input
        else:
            print("Invalid category. Please try again.")

def get_category_from_api(row):
    category = row[c.CATEGORY]

    if pd.isna(category):
        business = row[c.BUSINESS_OR_PERSON]
        business_original = row[c.BUSINESS_OR_PERSON_ORIGINAL]

        client = OpenAI()

        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an experienced business analyst who speaks every language and can find businesses using descriptions from credit card transactions. Use provided business descriptions to categorize transactions based on the name a business provides to the transaction. If you can't decide between one or more, pick the category that is more specific. If no category fits, return 'no category'. This list contains the category along with a description in parenthesis: groceries (), home (Any home improvements or furniture), learning (Businesses that sells books or provide teaching services like language tutoring), dining (restaurants, bakeries, cafes, kiosks, etc.), entertainment (All forms of entertainment including concerts, movies, sports games, etc.), exercise (gym, swimming, sports stores, bike stores), car/bike/metro (Public transportation used within a city, scooter/bike rental services, ride-sharing services like Uber/Lyft, or anything related to car services like gas, car parts, or car repairs), travel (Any travel from one city to another including trains, flights, and hotels/airbnbs), utilities (mobile phone related coses, internet, electricity, water, etc.), health care (hospitals, pharmacies, etc.), insurance (), pet care (pet stores), donation (Non-profits), merchandise (Purchases like clothes, online purchases, etc.)."},
                {"role": "user", "content": f"What is the category for '{business_original}'? Please only respond with the category."}
            ],
            temperature=0.1 
        )

        updated_category = completion.choices[0].message.content

        if CATEGORIES_DF[c.CATEGORY].str.contains(updated_category).any():
            print(f"Chat GPT labeled {business} as {updated_category}")
        else:
            print()
            print(f"CHATGPT could not categorize the business correctly: {business_original}")
            print(f"Original category: {row[c.CATEGORY_ORIGINAL]}")
            print(f"Amount: ${row[c.DEBIT]}")
            updated_category = get_valid_category()
            
        # Add new category to business:category mappings.
        new_record = pd.DataFrame({c.BUSINESS_OR_PERSON: [business], c.CATEGORY: [updated_category]})

        categorized_businesses_df = pd.read_csv(c.DATA_DIRECTORY_PATH + 'categorized_businesses.csv')
        categorized_businesses_df = pd.concat([categorized_businesses_df, new_record], ignore_index=True)
        categorized_businesses_df = categorized_businesses_df.drop_duplicates()
        categorized_businesses_df.to_csv(c.DATA_DIRECTORY_PATH + 'categorized_businesses.csv', index=False)

        return updated_category
    else:
        return category

def check_for_approved_categories(df):
    """Checks if DataFrame contains approved categories."""
    categories_set = set(CATEGORIES_DF[c.CATEGORY])
    unique_categories_df = set(df[c.CATEGORY])
    categories_not_in_csv = unique_categories_df - categories_set

    if categories_not_in_csv:
        print("The DataFrame contains categories not existing in the 'categories.csv' file:")
        print(categories_not_in_csv)
    else:
        print("Dataframe contains no unapproved categories. Import successful!")

# Load

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
        transactions_df = pd.concat([transactions_df, df])

    if transactions_df.empty:
        return
    else:
        # Get today's date
        today_date = datetime.today().strftime('%Y-%m-%d')

        # Create filename with today's date
        filename = f"transactions_{today_date}.csv"
        transactions_df.to_csv(c.IMPORTED_TRANSACTIONS_DIRECTORY_PATH + filename, index=False)

        # Update missing categories using llm
        transactions_df['category'] = transactions_df.apply(get_category_from_api, axis=1)

        # Add rows from transaction history to current transactions data frame.
        if os.path.exists(c.TRANSACTIONS_HISTORY_FILE_PATH):
            transaction_history_df = pd.read_csv(c.TRANSACTIONS_HISTORY_FILE_PATH)
            transactions_df = pd.concat([transactions_df, transaction_history_df], ignore_index=True)

        # Remove transactions that have already been added.
        transactions_df = transactions_df.drop_duplicates(subset=[c.DATE, c.BUSINESS_OR_PERSON_ORIGINAL, c.DEBIT, c.SEQUENCE])

        # Sort by date.
        transactions_df = transactions_df.sort_values(by=[c.DATE, c.CATEGORY, c.BUSINESS_OR_PERSON], ascending=[False, True, True])

        # Save for long-term storage.
        transactions_df.to_csv(c.TRANSACTIONS_HISTORY_FILE_PATH, index=False)

        check_for_approved_categories(transactions_df)
        send_csv_files_to_trash()

if __name__ == "__main__":
    main()