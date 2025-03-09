"""
Insertion strategies package.

This package contains strategies for inserting data into the database.
Each strategy implements the InsertionStrategy interface and provides
different approaches to data insertion.
"""

from src.db.strategies.insertion_strategy import InsertionStrategy
from src.db.strategies.bulk_insertion import BulkInsertionStrategy
from src.db.strategies.individual_insertion import IndividualInsertionStrategy

__all__ = [
    'InsertionStrategy',
    'BulkInsertionStrategy',
    'IndividualInsertionStrategy',
]