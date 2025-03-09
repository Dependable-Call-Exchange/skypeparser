"""
Insertion strategies package.

This package contains strategies for inserting data into the database.
Each strategy implements the InsertionStrategy interface and provides
different approaches to data insertion.
"""

from src.core_utils.insertion_strategy import InsertionStrategy
from src.core_utils.bulk_insertion import BulkInsertionStrategy
from src.core_utils.individual_insertion import IndividualInsertionStrategy

__all__ = [
    'InsertionStrategy',
    'BulkInsertionStrategy',
    'IndividualInsertionStrategy',
]