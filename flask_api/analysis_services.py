import pandas as pd
# from mongo_service.repository import get_attack_type, get_location, get_group

from mongo_service.db import collection

data = list(collection.find())


def extract_value_from_dict(data, key, default=0):
    return data.get(key, default) if isinstance(data, dict) else default


def create_dataframe_from_collection():
    """Fetch data from MongoDB and convert it into a Pandas DataFrame."""
    data = list(collection.find())
    return pd.DataFrame(data)


def order_by_attack_types_deadliest():

    df = create_dataframe_from_collection()

    df['number_kill'] = df['casualties'].apply(lambda x: x.get('number_kill', 0))
    df['number_wound'] = df['casualties'].apply(lambda x: x.get('number_wound', 0))

    df = df[df['attack_type'] != 'Unknown']

    aggregated_data = (df.groupby('attack_type')
    .agg(
        total_kill=('number_kill', 'sum'),
        total_wound=('number_wound', 'sum')
    ).reset_index()
    )

    aggregated_data['score'] = aggregated_data['total_kill'] * 2 + aggregated_data['total_wound']
    sorted_data = aggregated_data[['attack_type', 'score']].sort_values(by='score', ascending=False)
    return sorted_data.to_dict(orient='records')

def fcalculate_top_countries_by_casualties():

    df = create_dataframe_from_collection()

    df['country'] = df['location'].apply(lambda x: x.get('country', None))
    df['number_kill'] = df['casualties'].apply(lambda x: x.get('number_kill', 0))
    df['number_wound'] = df['casualties'].apply(lambda x: x.get('number_wound', 0))

    aggregated_data = (df.groupby('country')
    .agg(
        average_kill=('number_kill', 'mean'),
        average_wound=('number_wound', 'mean')
    ).reset_index()
    )

    aggregated_data['score'] = aggregated_data['average_kill'] * 2 + aggregated_data['average_wound']
    sorted_data = aggregated_data[['country', 'score']].sort_values(by='score', ascending=False)
    return sorted_data.to_dict(orient='records')

def find_top_5_group_by_casualties():

    df = create_dataframe_from_collection()

    df['number_kill'] = df['casualties'].apply(lambda x: x.get('number_kill', 0))
    df['number_wound'] = df['casualties'].apply(lambda x: x.get('number_wound', 0))

    df = df[df['target_type'] != 'Unknown']

    aggregated_data = (df.groupby('target_type')
    .agg(
        total_kill=('number_kill', 'sum'),
        total_wound=('number_wound', 'sum')
    ).reset_index()
    )

    aggregated_data['score'] = aggregated_data['total_kill'] * 2 + aggregated_data['total_wound']
    sorted_data = aggregated_data[['target_type', 'score']].sort_values(by='score', ascending=False)

    top_five = sorted_data.head(5)
    return top_five.to_dict(orient='records')


def calc_diff_percentage_by_year_and_country():

    df = create_dataframe_from_collection()

    df = df[df['group'] != 'Unknown']

    df['year'] = df['date_event'].dt.year
    df['country'] = df['location'].apply(lambda x: x.get('country', None))

    aggregated_data = (df.groupby('year')
    .agg(
        count_events=('event_id', 'count')
    ).reset_index()
    )

    sorted_data = aggregated_data[['year', 'count_events']].sort_values(by='year')

    sorted_data['diff_percentage'] = sorted_data['count_events'].pct_change()
    return sorted_data.to_dict(orient='records')


def find_most_active_groups_by_country():

    df = create_dataframe_from_collection()

    df['country'] = df['location'].apply(lambda x: x.get('country', None))

    aggregated_data = (df.groupby(['country', 'group'])
    .agg(
        count_events=('event_id', 'count')
    ).reset_index()
    )

    sorted_data = aggregated_data[['country', 'group', 'count_events']].sort_values(by='count_events', ascending=False)

    return sorted_data.to_dict(orient='records')