import json
from geopy.geocoders import Nominatim

import pandas as pd
#from geopy import Nominatim


NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")




from bs4 import BeautifulSoup

def get_wikipedia_data(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_=["wikitable", "sortable"])
    
    if not tables:
        raise ValueError("No 'wikitable sortable' tables found.")
    
    table = tables[1]
    rows = table.find_all("tr")
    return rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')



def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    if not html:
        return []

    rows = get_wikipedia_data(html)

    data = []
    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        if len(tds) < 7:
            continue  # skip incomplete rows

        values = {
            'rank': i,
            'stadium':clean_text( tds[0].text.strip()),
            'capacity':clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region':clean_text( tds[2].text.strip()),
            'country': clean_text(tds[3].text.strip()),
            'city':clean_text( tds[4].text.strip()),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else NO_IMAGE,
            'home_team':clean_text( tds[6].text.strip()),
        }
        data.append(values)

   
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"



def get_lat_long(country, city):
    geolocator = Nominatim(user_agent='your_custom_app_name_airflow_v1')  # <-- Add a custom user agent
    try:
        location = geolocator.geocode(f'{city}, {country}', timeout=10)
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Geocoding failed for {city}, {country}: {e}")
    return None



def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"




def write_wikipedia_data(**kwargs):
    import json
    import pandas as pd
    from datetime import datetime
    from dotenv import load_dotenv
    import os

    # Load environment variables from .env file
    load_dotenv()

    # Pull data from XCom, parse to DataFrame
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    # Create filename with timestamp
    now = datetime.now()
    file_name = f'stadium_cleaned_{now.date()}_{now.time().strftime("%H_%M_%S")}.csv'

    # Save CSV locally
    local_path = f'data/{file_name}'
    data.to_csv(local_path, index=False)

    # Save CSV to Azure Data Lake Storage Gen2
    storage_options = {
        'account_key': os.getenv('AZURE_ACCOUNT_KEY')
    }
    adls_path = f'abfs://football-data-eng@footballdatastr.dfs.core.windows.net/data/{file_name}'

    data.to_csv(adls_path, storage_options=storage_options, index=False)
