import pandas as pd
import folium
from mongo_service.db import collection
import os


def create_dataframe_from_collection():
    data = list(collection.find())
    df = pd.DataFrame(data)

    df['year'] = df['date_event'].dt.year
    df['country'] = df['location'].apply(lambda x: x.get('country', None))
    df['longitude'] = df['location'].apply(lambda x: x.get('coordinates', None).get('longitude', None))
    df['latitude'] = df['location'].apply(lambda x: x.get('coordinates', None).get('latitude', None))
    df['casualties'] = df['casualties'].apply(
        lambda x: (x.get('number_wound', 0) or 0) + (x.get('number_kill', 0) or 0) * 2)
    return df


def generate_color_map(categories):
    """Generate a unique color for each category."""
    unique_categories = categories.unique()
    color_map = {category: plt.cm.tab20(i / len(unique_categories)) for i, category in enumerate(unique_categories)}
    # Convert RGB to HEX
    return {category: f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}" for category, (r, g, b, _) in color_map.items()}


def load_map(data, lat_col='latitude', lon_col='longitude', popup_col=None, color='blue'):
    m = folium.Map(location=[0, 0], zoom_start=2)

    for _, row in data.iterrows():
        if pd.notna(row[lat_col]) and pd.notna(row[lon_col]) and pd.notna(row[popup_col]):
            popup_text = row[popup_col] if popup_col and popup_col in row else ""
            # color = get_color(row['score'])
            color = 'blue'

            folium.CircleMarker(
                location=[row[lat_col], row[lon_col]],
                radius=10,
                popup=popup_text,
                color='green',
                fill=True,
                fill_opacity=0.7
            ).add_to(m)

    static_folder = os.path.join("flask_api", "static", "maps")
    map_file = os.path.join(static_folder, "map.html")
    m.save(map_file)
    return "/static/maps/map.html"



def order_by_attack_types_deadliest(top_five=None):

    df = create_dataframe_from_collection()

    df = df[df['attack_type'] != 'Unknown']

    aggregated_data = (df.groupby('attack_type')
    .agg(
        score=('casualties', 'sum')
    ).reset_index()
    )

    sorted_data = aggregated_data[['attack_type', 'score']].sort_values(by='score', ascending=False)

    if top_five is not None:
        sorted_data = sorted_data.head(5)

    return sorted_data.to_dict(orient='records')


def calculate_top_countries_by_casualties(top_five=None):

    df = create_dataframe_from_collection()

    aggregated_data = (df.groupby('country')
    .agg(
        average_casualties=('casualties', 'mean'),
        longitude=('longitude', 'first'),
        latitude=('latitude', 'first')
    ).reset_index()
    )

    sorted_data = (aggregated_data[['country', 'average_casualties', 'longitude', 'latitude']].
                   sort_values(by='average_casualties', ascending=False))

    sorted_data['popup'] = sorted_data.apply(
        lambda x: f"Country: {x['country']}<br>Score average: {x['average_casualties']:.2%}",
        axis=1
    )

    if top_five is not None:
        sorted_data = sorted_data.head(5)
    map_file = load_map(sorted_data, lat_col='latitude', lon_col='longitude', popup_col='popup')
    return map_file
    # return sorted_data.to_dict(orient='records')

def find_top_5_group_by_casualties():

    df = create_dataframe_from_collection()

    df = df[df['target_type'] != 'Unknown']

    aggregated_data = (df.groupby('target_type')
    .agg(
        total_casualties=('casualties', 'sum')
    ).reset_index()
    )

    sorted_data = (aggregated_data[['target_type', 'total_casualties']].
                   sort_values(by='total_casualties', ascending=False))

    top_five = sorted_data.head(5)
    return top_five.to_dict(orient='records')


def calc_diff_percentage_by_year_and_country():

    df = create_dataframe_from_collection()

    df = df[df['group'] != 'Unknown']

    country_coords = (
        df.groupby('country')
        .agg(
            longitude=('longitude', 'first'),
            latitude=('latitude', 'first')
        )
        .reset_index()
    )

    aggregated_data = (df.groupby(['country', 'year'])
    .agg(
        count_events=('event_id', 'count')
    ).reset_index()
    )

    def calculate_pct_change(group):
        group['diff_percentage'] = group['count_events'].pct_change()
        return group

    sorted_data = aggregated_data.groupby('country').apply(calculate_pct_change)

    sorted_data = sorted_data.reset_index(drop=True)

    avg_diff_percentage = sorted_data.groupby('country')['diff_percentage'].mean().reset_index()
    avg_diff_percentage = avg_diff_percentage.merge(
        country_coords,
        on='country',
        how='left'
    )
    avg_diff_percentage = avg_diff_percentage.rename(columns={'diff_percentage': 'avg_diff_percentage'})
    avg_diff_percentage['popup'] = avg_diff_percentage.apply(
        lambda x: f"Country: {x['country']}<br>Average Change: {x['avg_diff_percentage']:.2%}",
        axis=1
    )

    map_file = load_map(avg_diff_percentage, lat_col='latitude', lon_col='longitude', popup_col='popup')
    return map_file
    # return sorted_data.to_dict(orient='records')


def find_most_active_groups_by_country(country=None):

    df = create_dataframe_from_collection()

    aggregated_data = (df.groupby(['country', 'group'])
    .agg(
        count_events=('event_id', 'count')
    ).reset_index()
    )

    max_per_country = aggregated_data.loc[aggregated_data.groupby('country')['count_events'].idxmax()]
    max_per_country['popup'] = max_per_country.apply(
        lambda x: f"Country: {x['country']}<br>Group: {x['group']}<br>Events: {x['count_events']}",
        axis=1
    )

    # map_file = load_map(max_per_country, lat_col='latitude', lon_col='longitude', popup_col='popup', color='green')
    # return map_file

    sorted_data = aggregated_data[['country', 'group', 'count_events']].sort_values(by='count_events', ascending=False)

    return sorted_data.to_dict(orient='records')


def find_max_groups_with_common_target_by_country(country):
    df = create_dataframe_from_collection()

    df = df[df['country'].str.lower() == country.lower()]

    if df.empty:
        return f"No rows found for country: {country}"

    grouped = df.groupby(['target_type', 'country']).agg({
    'group': ['nunique', lambda x: list(x.unique())]
    }).reset_index()

    grouped.columns = ['target_type', 'country', 'unique_groups', 'group_names']

    max_group = grouped[grouped['unique_groups'] == grouped['unique_groups'].max()]

    return max_group.to_dict(orient='records')


def find_max_groups_with_common_attack_type_by_country(country):
    df = create_dataframe_from_collection()

    df = df[df['country'].str.lower() == country.lower()]

    if df.empty:
        return f"No rows found for country: {country}"

    grouped = df.groupby(['attack_type', 'country']).agg({
    'group': ['nunique', lambda x: list(x.unique())]
    }).reset_index()

    grouped.columns = ['attack_type', 'country', 'unique_groups', 'group_names']

    max_group = grouped[grouped['unique_groups'] == grouped['unique_groups'].max()]

    return max_group.to_dict(orient='records')