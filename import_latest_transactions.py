import subprocess

def main():

    cash_transactions_script_path = "cash_transactions_storage.py"
    subprocess.run(["python3", cash_transactions_script_path])

    transaction_labeling_and_storage_script_path = "transaction_labeling_and_storage.py"
    subprocess.run(["python3", transaction_labeling_and_storage_script_path])

if __name__ == "__main__":
    main()