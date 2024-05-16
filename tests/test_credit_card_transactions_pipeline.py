import unittest
import pandas as pd
from pandera.errors import SchemaErrors

import accounting.constant as c
from accounting.pipelines.credit_card_transactions_pipeline import CreditCardTransactionsPipeline
import accounting.tool as tool


class TestCreditCardTransactionsPipeline(unittest.TestCase):
    def setUp(self):
        self.credit_card_transactions_pipeline = CreditCardTransactionsPipeline([])
        self.invalid_transactions_file_name = 'tests/mock_data/invalid_capital_one_transactions.csv'
        self.temp_file_paths = []


    def test_invalid_transactions_csv_format(self):
        df = self.credit_card_transactions_pipeline.extract_capital_one_transactions(file_path=self.invalid_transactions_file_name)

        with self.assertRaises(SchemaErrors):
            self.credit_card_transactions_pipeline.clean_capital_one_transactions(df)
    
    def test_invalid_column_loaded_to_transactions_history(self):
        df = pd.read_csv(self.invalid_transactions_file_name, encoding='latin-1')

        temp_file_path = 'tests/mock_data/invalid_column_in_transactions.csv'
        self.temp_file_paths.append(temp_file_path)

        with self.assertRaises(SchemaErrors):
            self.credit_card_transactions_pipeline.load_transactions(df, temp_file_path)

    def tearDown(self):
        tool.send_to_trash(self.temp_file_paths)

if __name__ == '__main__':
    unittest.main()