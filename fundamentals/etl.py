"""
Python Extract, Transform, Load (ETL) EXAMPLE
"""

# Import packages

#%%
import requests
import pandas as pd
from sqlalchemy import create_engine

#%%
# Extract function
def extract() -> dict:
    """
    Extract data from a public API.
    This API extract data from
    https://universities.hipolaps.com
    """
    # this gives us a data in json format
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data 
#%%
# Transform function
# Consider Hypothetical data. 
    # like get the total number of universities in california
def transform(data: dict) -> pd.DataFrame:
    """ Transform the data into designed structure and filters """
    df = pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    # Filter data for California
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in California: {len(df)}")
    df ['domains'] = [','.join(map(str,l)) for l in df['domains']]
    df ['web_pages'] = [','.join(map(str,l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]

#%%
def load(df:pd.DataFrame) -> None:
    """ Loads data into a sqlite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists="replace")

#%%
# Execute the ETL process
data = extract()
df = transform(data)
load(df)
df
