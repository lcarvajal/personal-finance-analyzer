import unittest
import pandas as pd
from pandera.errors import SchemaErrors

import accounting.constant as c
from accounting.pipelines.credit_card_transactions_pipeline import CreditCardTransactionsPipeline


class TestCreditCardTransactionsPipeline(unittest.TestCase):
    def setUp(self):
        self.credit_card_transactions_pipeline = CreditCardTransactionsPipeline([])
        # transaction_file_paths='tests/mock_data/valid_capital_one_transactions.csv'


    def test_unexpected_csv_format(self):
        invalid_transactions_file_name = 'tests/mock_data/invalid_capital_one_transactions.csv'
        df = self.credit_card_transactions_pipeline.extract_capital_one_transactions(file_path=invalid_transactions_file_name)

        with self.assertRaises(SchemaErrors):
            self.credit_card_transactions_pipeline.clean_capital_one_transactions(df)
            

if __name__ == '__main__':
    unittest.main()