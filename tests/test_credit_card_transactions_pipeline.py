import unittest
import accounting.constant as c
import accounting.credit_card_transactions_pipeline as cc_pipeline
import pandas as pd

class TestCreditCardTransactionsPipeline(unittest.TestCase):
    def setUp(self):
        self.extracted_transactions = pd.read_csv('mock_data/valid_capital_one_transactions.csv', encoding='latin-1')
        self.invalid_transactions_file_name = 'mock_data/invalid_capital_one_transactions.csv'


    def test_unexpected_csv_format(self):
        with self.assertRaises(TypeError):
            cc_pipeline.extract_capital_one_transactions(self.invalid_transactions_file_name)


if __name__ == '__main__':
    unittest.main()