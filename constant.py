# Environment variable keys
OPEN_AI_KEY = 'OPENAI_API_KEY'
CASH_TRANSACTIONS_DATABASE_ID_KEY = 'CASH_TRANSACTIONS_DATABASE_ID'
NOTION_API_KEY = 'NOTION_API_KEY'

# Dataframe columns
BUSINESS_OR_PERSON = 'business_or_person'
BUSINESS_OR_PERSON_ORIGINAL= 'business_or_person_original'
CARD_NUMBER = 'card_number'
CATEGORY = 'category'
CATEGORY_ORIGINAL = 'category_original'
CREDIT = 'credit'
DATE = 'date'
DEBIT = 'debit'
SEQUENCE = 'sequence'

# Directory and file paths
DATA_DIRECTORY_PATH = 'data/'
TEMP_DIRECTORY_PATH = DATA_DIRECTORY_PATH + 'temp/'
IMPORTED_TRANSACTIONS_DIRECTORY_PATH = DATA_DIRECTORY_PATH + 'imported_transactions/'
TRANSACTIONS_HISTORY_FILE_PATH = DATA_DIRECTORY_PATH + 'transactions_history.csv'

# Prompts
CATEGORIZE_TRANSACTION_PROMPT = "You are an experienced business analyst who speaks every language and can find businesses using descriptions from credit card transactions. Use provided business descriptions to categorize transactions based on the name a business provides to the transaction. If you can't decide between one or more, pick the category that is more specific. If no category fits, return 'no category'. This list contains the category along with a description in parenthesis: groceries (), home (Any home improvements or furniture), learning (Businesses that sells books or provide teaching services like language tutoring), dining (restaurants, bakeries, cafes, kiosks, etc.), entertainment (All forms of entertainment including concerts, movies, sports games, etc.), exercise (gym, swimming, sports stores, bike stores), car/bike/metro (Public transportation used within a city, scooter/bike rental services, ride-sharing services like Uber/Lyft, or anything related to car services like gas, car parts, or car repairs), travel (Any travel from one city to another including trains, flights, and hotels/airbnbs), utilities (mobile phone related coses, internet, electricity, water, etc.), health care (hospitals, pharmacies, etc.), insurance (), pet care (pet stores), donation (Non-profits), merchandise (Purchases like clothes, online purchases, etc.)."