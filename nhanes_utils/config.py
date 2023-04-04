"""
Configuration file for global variables required by nhanes_utils modules.

Toby Rea
04-04-2023
"""

URL = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component="
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"}
DATA_DIRECTORY = "data"
COMPONENTS = [
    "Demographics",
    "Dietary",
    "Examination",
    "Laboratory",
    "Questionnaire"
]
YEARS = [
    "1999-2000",
    "2001-2002",
    "2003-2004",
    "2005-2006",
    "2007-2008",
    "2009-2010",
    "2011-2012",
    "2013-2014",
    "2015-2016",
    "2017-2018",
    "2017-2020",
    "2019-2020",
    "2021-2022"
]
