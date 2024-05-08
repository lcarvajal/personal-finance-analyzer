# Accounting

## About
This program imports credit card transactions and categorizes them in a way Lukas wants. The categorized data can then be used to analyze in `analysis.ipynb`.

- Credit card transaction data is supported for Capital One.
- The data gets categorized based on mappings done by Lukas. If the mapping for a business doesn't exist, it uses the openai API to categorize it.

## Setup
1. Create a virtual environment: `python -m venv accoutningenv`
2. (VSCode) Install ipykernal: `pip install ipykernel`
3. Create a Jupyter Kernal: `python -m ipykernel install --user --name=accoutningenv` 

## How to run
1. Activate environment: `source accountingenv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`
3. Place downloaded transactions in `data/temp/`
3. Run `python import_latest_transactions.py`

## Extra commands
- Deactivate environment: `deactivate`
- Display installed packages: `pip list`
- Capture current dependencies: `pip freeze > requirements.txt`

## To-dos
1. Add support for inputting cash transactions on Notion
2. Create unit tests to ensure program works as expected
3. Add support for Citi Card
4. Add support for cash transactions