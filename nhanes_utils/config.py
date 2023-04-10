CHUNK_SIZE = 32 * 1024 * 1024
SCRAPER_BASE_URL = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component="
COMPONENTS = ["Demographics", "Dietary", "Examination", "Laboratory", "Questionnaire"]
MAX_CONCURRENT_DOWNLOADS = 5
DOWNLOAD_RETRIES = 3
DATASETS_CSV = "datasets.csv"
DOWNLOAD_DIRECTORY = "datasets"
YEARS = [
    "1999-2000",
    "1999-2004",
    "2001-2002",
    "2003-2004",
    "2005-2006",
    "2007-2008",
    "2007-2012",
    "2009-2010",
    "2011-2012",
    "2013-2014",
    "2015-2016",
    "2017-2018",
    "2017-2020",
    "2019-2020",
    "2021-2022"
]
