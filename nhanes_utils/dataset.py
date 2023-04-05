"""
Data class that models metadata for a NHANES dataset.

Toby Rea
05-04-2023
"""

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class Dataset:
    years: str
    component: str
    description: str
    data_url: str
    docs_url: str
