from openai import OpenAI
import os
import pandas as pd
from dotenv import load_dotenv

import accounting.constant as c

load_dotenv()
OPENAI_API_KEY = os.getenv(c.OPEN_AI_KEY)


def extract_categories():
    df = pd.read_csv(c.DATA_DIRECTORY_PATH + 'categories.csv')
    column_to_drop = 'Unnamed: 1'
    if column_to_drop in df.columns:
        df.drop(column_to_drop, axis=1, inplace=True)
    pd.set_option('display.max_rows', None)
    print("Valid Categories:")
    print(df)
    pd.set_option('display.max_rows', 60)
    return df


categories_df = extract_categories()


def get_valid_categories():
    return set(categories_df[c.CATEGORY])


def categorize_transactions(df):
    df[c.CATEGORY] = df[c.CATEGORY].str.lower()

    # Merge transactions with categorized_businesses_df to get correct categories
    categorized_businesses_df = pd.read_csv(
        c.DATA_DIRECTORY_PATH + 'categorized_businesses.csv')
    df = pd.merge(df, categorized_businesses_df,
                  on=c.BUSINESS_OR_PERSON, how='left')

    df = df.rename(columns={
        'category_x': c.CATEGORY_ORIGINAL,
        'category_y': c.CATEGORY})

    # Merge with categories to get the correct category names
    df = pd.merge(df, categories_df, on=c.CATEGORY, how='left')

    return df


def get_category_from_api(row):
    category = row[c.CATEGORY]

    if pd.isna(category):
        business = row[c.BUSINESS_OR_PERSON]
        business_original = row[c.BUSINESS_OR_PERSON_ORIGINAL]

        client = OpenAI()

        try:
            completion = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": c.CATEGORIZE_TRANSACTION_PROMPT},
                    {"role": "user", "content": f"What is the category for '{business_original}'? Please only respond with the category."}
                ],
                temperature=0.1
            )

            updated_category = completion.choices[0].message.content

            if categories_df[c.CATEGORY].str.contains(updated_category).any():
                print(f"Chat GPT labeled {business} as {updated_category}")
            else:
                print()
                print(
                    f"CHATGPT could not categorize the business correctly: {business_original}")
                print(f"Original category: {row[c.CATEGORY_ORIGINAL]}")
                print(f"Amount: ${row[c.DEBIT]}")
                updated_category = get_valid_category_from_user()

            load_business_to_category_mapping(business, updated_category)

            return updated_category
        except Exception as e:
            print()
            print(
                f"Error occurred while trying to categorize {business_original}: {e}")
            print()
            print(
                f"CHATGPT could not categorize the business correctly: {business_original}")
            print(f"Original category: {row[c.CATEGORY_ORIGINAL]}")
            print(f"Amount: ${row[c.DEBIT]}")
            updated_category = get_valid_category_from_user()

            return updated_category
    else:
        return category


def get_valid_category_from_user():
    while True:
        user_input = input("Enter a category: ").strip().lower()

        if user_input in categories_df[c.CATEGORY].str.lower().values:
            return user_input
        else:
            print("Invalid category. Please try again.")


def load_business_to_category_mapping(business, category):
    new_record = pd.DataFrame(
        {c.BUSINESS_OR_PERSON: [business], c.CATEGORY: [category]})
    categorized_businesses_df = pd.read_csv(
        c.DATA_DIRECTORY_PATH + 'categorized_businesses.csv')
    categorized_businesses_df = pd.concat(
        [categorized_businesses_df, new_record], ignore_index=True)
    categorized_businesses_df = categorized_businesses_df.drop_duplicates()
    categorized_businesses_df.to_csv(
        c.DATA_DIRECTORY_PATH + 'categorized_businesses.csv', index=False)
