from dataclasses import dataclass


@dataclass(order=True, frozen=True)
class Dataset:
    """ A class representing a dataset. """

    years: str
    component: str
    description: str
    docs_url: str
    data_url: str
