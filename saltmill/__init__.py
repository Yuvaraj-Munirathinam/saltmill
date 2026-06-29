"""
saltmill — Efficient large CSV processing for PySpark and Databricks.

Automatic salt-based partitioning, schema inference, and Spark tuning
so developers can read 500GB+ CSV files with a single call.
"""

from .core import SaltMill
from .reader import read

__version__ = "0.1.0"
__all__ = ["SaltMill", "read"]
