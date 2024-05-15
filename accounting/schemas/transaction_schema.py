import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series

from accounting import constant as c
from accounting.transaction_category import get_valid_categories

class TransactionSchema(pa.DataFrameModel):
    date: object
    card_number: int
    business_or_person_original: object
    category_original: object
    debit: float
    business_or_person: object
    category: object = pa.Field(isin=get_valid_categories())
    sequence: int

class CapitalOneTransactionSchema(pa.DataFrameModel):
    transaction_date: object = pa.Field(alias=c.CAP_ONE_TRANSACTION_DATE)
    posted_date: object = pa.Field(alias=c.CAP_ONE_POSTED_DATE)
    card_number: int = pa.Field(alias=c.CAP_ONE_CARD_NUMBER)
    description: object = pa.Field(alias=c.CAP_ONE_DESCRIPTION)
    category: object = pa.Field(alias=c.CAP_ONE_CATEGORY)
    debit: float = pa.Field(alias=c.CAP_ONE_DEBIT, nullable=True)
    credit: float = pa.Field(alias=c.CAP_ONE_CREDIT, nullable=True)
