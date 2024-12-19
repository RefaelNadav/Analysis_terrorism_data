from db import collection
import csv
from datetime import datetime
from uuid import uuid4

csv_path = '../data/globalterrorismdb_0718dist.csv'

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
   # collection.drop()


    for row in read_csv(csv_path):
        year = int(row['iyear'])
        month = int(row['imonth'])
        day = int(row['iday'])
        if month == 0:
           month = 1
        if day == 0:
            day = 1

        event = {
            'event_id': str(uuid4()),  # שמירה כמחרוזת כדי להבטיח תאימות למונגו
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

init_events()
       #
       # injuries = {
       #     'injuries_total': validate_and_convert_int(row['INJURIES_TOTAL']),
       #     'injuries_fatal': validate_and_convert_int(row['INJURIES_FATAL']),
       #     'injuries_non_fatal': validate_and_convert_int(row['INJURIES_TOTAL']) - \
       #                           validate_and_convert_int(row['INJURIES_FATAL'])
       #
       # }
       #
       #
       # accident = {
       #     'beat_of_occurrence': row['BEAT_OF_OCCURRENCE'],
       #     'crash_date': parse_date(row['CRASH_DATE']),
       #     'prim_contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
       #     'injuries': injuries
       # }
       #
       # accidents.insert_one(accident)
