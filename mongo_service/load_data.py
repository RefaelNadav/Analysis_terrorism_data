from db import collection
import csv
from datetime import datetime
from uuid import uuid4

csv_path_1 = '../data/globalterrorismdb_0718dist.csv'
csv_path_2 = '../data/RAND_Database_of_Worldwide_Terrorism_Incidents.csv'

def read_csv(csv_path):
   with open(csv_path, encoding="latin-1") as file:
       csv_reader = csv.DictReader(file)
       for row in csv_reader:
           yield row

def validate_and_convert_int(value):
    if value == '':
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def validate_and_convert_float(value):
    if value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None



def init_events():


    for row in read_csv(csv_path_1):
        year = int(row['iyear'])
        month = int(row['imonth'])
        day = int(row['iday'])
        if month == 0:
           month = 1
        if day == 0:
            day = 1

        event = {
            'event_id': str(uuid4()),
            'date_event': datetime(year, month, day),
            'group': row.get('gname'),
            'attack_type': row.get('attacktype1_txt'),
            'target_type': row.get('targtype1_txt'),
            'location': {
                'country': row.get('country_txt'),
                'region': row.get('region_txt'),
                'coordinates': {
                    'longitude': validate_and_convert_float(row.get('longitude')),
                    'latitude': validate_and_convert_float(row.get('latitude')),
                }
            },
            'casualties': {
                'number_kill': validate_and_convert_int(row.get('nkill')),
                'number_wound': validate_and_convert_int(row.get('nwound')),
            },
            'number_perps': validate_and_convert_int(row.get('nperps'))
        }

        collection.insert_one(event)


        print(event)


def get_coordinates_by_country(country_name):
    existing_event = collection.find_one(
        {
            "location.country": country_name,
            "location.coordinates.longitude": {"$ne": None},
            "location.coordinates.latitude": {"$ne": None}
        },
        {"location.coordinates": 1}
    )

    if existing_event:
        return existing_event['location']['coordinates']



def marge_new_data():
    queries = []
    for row in read_csv(csv_path_2):
        coordinates = get_coordinates_by_country(row['Country'])
        if coordinates is not None:
            event = {
                'event_id': str(uuid4()),
                'date_event': row['Date'],
                'group': row.get('Perpetrator'),
                'attack_type': row.get('Weapon'),
                'target_type': None,
                'location': {
                    'country': row.get('Country'),
                    'region': None,
                    'coordinates': coordinates
                },
                'casualties': {
                    'number_kill': validate_and_convert_int(row.get('Injuries ')),
                    'number_wound': validate_and_convert_int(row.get('Fatalities ')),
                },
                'number_perps': None
            }
            collection.insert_one(event)


