# imports
import pandas as pd
from datetime import timedelta
from geopy.distance import great_circle

# dizionario che mappa i nomi dei borough ai loro acronimi
acronyms = {'BRONX': 'BX',
            'BROOKLYN': 'BK',
            'MANHATTAN': 'MN',
            'QUEENS': 'QN',
            'STATEN ISLAND': 'SI'}

# dizionario che mappa i codici dei distretti alle sub-borough area
districts_map = {'BK01': 'Williamsburg/Greenpoint',
                 'BK02': 'Brooklyn Heights/Fort Greene',
                 'BK03': 'Bedford Stuyvesant',
                 'BK04': 'Bushwick',
                 'BK05': 'East New York/Starrett City',
                 'BK06': 'Park Slope/Carroll Gardens',
                 'BK07': 'Sunset Park',
                 'BK08': 'North Crown Heights/Prospect Heights',
                 'BK09': 'South Crown Heights',
                 'BK10': 'Bay Ridge',
                 'BK11': 'Bensonhurst',
                 'BK12': 'Borough Park',
                 'BK13': 'Coney Island',
                 'BK14': 'Flatbush',
                 'BK15': 'Sheepshead Bay/Gravesend',
                 'BK16': 'Brownsville/Ocean Hill',
                 'BK17': 'East Flatbush',
                 'BK18': 'Flatlands/Canarsie',
                 'BX01': 'Mott Haven/Hunts Point',
                 'BX02': 'Mott Haven/Hunts Point',
                 'BX03': 'Morrisania/Belmont',
                 'BX04': 'Highbridge/South Concourse',
                 'BX05': 'University Heights/Fordham',
                 'BX06': 'Morrisania/Belmont',
                 'BX07': 'Kingsbridge Heights/Mosholu',
                 'BX08': 'Riverdale/Kingsbridge',
                 'BX09': 'Soundview/Parkchester',
                 'BX10': 'Throgs Neck/Co-op City',
                 'BX11': 'Pelham Parkway',
                 'BX12': 'Williamsbridge/Baychester',
                 'MN01': 'Greenwich Village/Financial District',
                 'MN02': 'Greenwich Village/Financial District',
                 'MN03': 'Lower East Side/Chinatown',
                 'MN04': 'Chelsea/Clinton/Midtown',
                 'MN05': 'Chelsea/Clinton/Midtown',
                 'MN06': 'Stuyvesant Town/Turtle Bay',
                 'MN07': 'Upper West Side',
                 'MN08': 'Upper East Side',
                 'MN09': 'Morningside Heights/Hamilton Heights',
                 'MN10': 'Central Harlem',
                 'MN11': 'East Harlem',
                 'MN12': 'Washington Heights/Inwood',
                 'QN01': 'Astoria',
                 'QN02': 'Sunnyside/Woodside',
                 'QN03': 'Jackson Heights',
                 'QN04': 'Elmhurst/Corona',
                 'QN05': 'Middle Village/Ridgewood',
                 'QN06': 'Rego Park/Forest Hills',
                 'QN07': 'Flushing/Whitestone',
                 'QN08': 'Hillcrest/Fresh Meadows',
                 'QN09': 'Ozone Park/Woodhaven',
                 'QN10': 'South Ozone Park/Howard Beach',
                 'QN11': 'Bayside/Little Neck',
                 'QN12': 'Jamaica',
                 'QN13': 'Queens Village',
                 'QN14': 'Rockaways',
                 'SI01': 'North Shore',
                 'SI02': 'Mid-Island',
                 'SI03': 'South Shore'}


def preprocess_servicerequests_data():
    """
    Esegue il preprocessing iniziale del dataset '311-2023-05.csv',
    ottenuto precedentemente dalla selezione delle richieste di servizio 311
    fatte nel mese di maggio dell'anno 2023, da cui sono state inoltre
    eliminate colonne a valori per la maggior parte nulli.
    """
    df = pd.read_csv('datasets/311-2023-05.csv')

    cols_to_drop = ['Vehicle Type', 'Taxi Company Borough']
    cols_to_normalize = ['Complaint Type', 'Descriptor', 'Location Type',
                         'Incident Address', 'Street Name', 'Status',
                         'Borough', 'Open Data Channel Type']
    
    normalize_string_spaces = lambda s: ' '.join(s.split())
    def transform_district_codes(s):
        number, borough = s.split(' ', 1)
        return f"{acronyms.get(borough)}{number}"
    
    df.drop(columns=cols_to_drop, inplace=True)
    df.rename(columns={'Community Board': 'Sub-Borough Area'}, inplace=True)

    df = df.dropna(subset=['Location'])
    for col in df.columns:
        df[col] = df[col].fillna('unknown')
    
    for col in cols_to_normalize:
        df[col] = df[col].apply(normalize_string_spaces)
        df[col] = df[col].str.lower()
    
    df['Sub-Borough Area'] = df['Sub-Borough Area'].apply(transform_district_codes).map(districts_map)

    df[['Latitude', 'Longitude']] = df['Location'].str.extract(r'\(([^,]+), ([^,]+)\)').astype(float)
    df.drop(columns=['Location'], inplace=True)

    df.to_csv('datasets/311-2023-05-v2.csv', index=False)


def create_unique_incidents_dataset():
    """
    Individua, a partire dal dataset preprocessato '311-2023-05-v2.csv',
    gli `incident` unici oggetto di più richieste di servizio,
    isolandoli e realizzando un dataset che li collezioni.
    """
    df = pd.read_csv('datasets/311-2023-05-v2.csv')
    df['Created Date'] = pd.to_datetime(df['Created Date'])

    def is_same_incident(row1, row2, time_window_hours=24, max_distance_meters=80):
        if row1['Complaint Type'] != row2['Complaint Type']:
            return False
        if row1['Descriptor'] != row2['Descriptor']:
            return False
        if row1['Location Type'] != row2['Location Type']:
            return False

        time_diff = abs((row1['Created Date'] - row2['Created Date']).total_seconds()) / 3600
        if time_diff > time_window_hours:
            return False
        
        distance = great_circle((row1['Latitude'], row1['Longitude']), (row2['Latitude'], row2['Longitude'])).meters
        if distance > max_distance_meters:
            return False
        
        return True
    
    def identify_incidents(df):
        incidents = []
        used_indexes = set()

        for i, row1 in df.iterrows():
            if i in used_indexes:
                continue
            incident = [i]
            for j, row2 in df.iterrows():
                if j != i and j not in used_indexes and is_same_incident(row1, row2):
                    incident.append(j)
            used_indexes.update(incident)
            incidents.append(incident)
        
        return incidents
    
    df['Incident Id'] = 0
    incidents = identify_incidents(df)
    incidents_rows = []

    for i, incident in enumerate(incidents):
        incident_id = i + 1
        incident_data = df.iloc[incident]
        incident_data['Incident Id'] = incident_id
        row = incident_data.iloc[0]
        incident_row = {'Incident Id': incident_id,
                        'Earliest Created Date': incident_data['Created Date'].min(),
                        'Complaint Type': row['Complaint Type'],
                        'Descriptor': row['Descriptor'],
                        'Borough': row['Borough'],
                        'Sub-Borough Area': row['Sub-Borough Area'],
                        'Incident Address': row['Incident Address'],
                        'Location Type': row['Location Type']}
        incidents_rows.append(incident_row)
    
    incidents_df = pd.DataFrame(incidents_rows)
    incidents_df.to_csv('datasets/311-unique-incidents.csv', index=False)

    df.drop(columns=['Complaint Type', 'Descriptor', 'Borough',
                     'Sub-Borough Area', 'Street Name',
                     'Incident Address', 'Location Type'], inplace=True)
    df.to_csv('datasets/311-2023-05-v3.csv', index=False)
    

# lista dei path dei dataset sui distretti
districts_data_paths = ['datasets/district-incomedistribution.csv',
                        'datasets/district-povertyrate.csv',
                        'datasets/district-racecomposition.csv',
                        'datasets/district-crimerate.csv']

# lista delle coppie di distretti i cui dati sono ripetuti
districts_to_collapse = [('BX01', 'BX02'),
                         ('BX03', 'BX06'),
                         ('MN01', 'MN02'),
                         ('MN04', 'MN05')]

# lista delle coppie (path, rinominazione della colonna 2021) per ciascuno dei dataset sui sub-borough
subboroughs_data_info = [('datasets/sub-borougharea-populationdensity1000personspersquaremile.csv', 'Population Density'),
                         ('datasets/sub-borougharea-populationaged65.csv', 'Population Aged 65+'),
                         ('datasets/sub-borougharea-borninnewyorkstate.csv', 'NYS Born People'),
                         ('datasets/sub-borougharea-foreign-bornpopulation.csv', 'Foreign Born People'),
                         ('datasets/sub-borougharea-disabledpopulation.csv', 'Disabled People'),
                         ('datasets/sub-borougharea-unemploymentrate.csv', 'Unemployment Rate'),
                         ('datasets/sub-borougharea-car-freecommuteofcommuters.csv', 'Car-Free Commuters'),
                         ('datasets/sub-borougharea-householdswithchildrenunder18yearsold.csv', 'Families with Children U18'),
                         ('datasets/sub-borougharea-populationaged25withabachelorsdegreeorhigher.csv', 'People O25 with Bachelor'),
                         ('datasets/sub-borougharea-populationaged25withoutahighschooldiploma.csv', 'People O25 without Diploma')]


def preprocess_subboroughs_data():
    """
    Esegue il preprocessing dei dataset sulle sub-borough area,
    ognuno contenente uno o più attributi di interesse. I dataset
    sono poi joinati tra loro per ottenere un unico dataset sui sub-boroughs.
    """
    dfs = []
    cols_to_drop = ['Name', 'Level', 'Year']

    for path in districts_data_paths:
        df = pd.read_csv(path)
        dfs.append(df)
    
    dfs[0].rename(columns={'year', 'Year'}, inplace=True)
    dfs[0] = dfs[0][dfs[0]['Year'] == '2018-2022']

    for df in dfs:
        df = df.iloc[6:]
        df.drop(columns=cols_to_drop, inplace=True)
        df.rename(columns={'Geography': 'Community District'}, inplace=True)
        df['Community District'] = df['Community District'].apply(lambda s: ''.join(s.split()))
    
    dfs[0].rename(columns={'property_crime_rate': 'Property Crime Rate',
                           'violent_crime_rate': 'Violent Crime Rate'}, inplace=True)
    dfs[1].rename(columns={'<= $20,000': 'Low Income Population',
                           '$20,001 -\n$40,000': 'Medium-Low Income Population',
                           '$40,001 -\n$60,000': 'Medium Income Population',
                           '$60,001 -\n$100,000': 'Medium-High Income Population',
                           '$100,001 -\n$250,000': 'High Income Population',
                           '> $250,000': 'Very High Income Population'}, inplace=True)
    dfs[2].rename(columns={'poverty_rate': 'Poverty Rate'}, inplace=True)
    dfs[3].rename(columns={'pop_hispanic_pct': 'Hispanic Population',
                           'pop_non_hispanic_asian_pct': 'Asian Population',
                           'pop_non_hispanic_black_pct': 'Black Population',
                           'pop_non_hispanic_white_pct': 'White Population'}, inplace=True)
    
    df_join = pd.merge(dfs[0], dfs[1], on='Community District', how='inner')
    df_join = pd.merge(df_join, dfs[2], on='Community District', how='inner')
    df_join = pd.merge(df_join, dfs[3], on='Community District', how='inner')

    for i, col in enumerate(df_join.columns):
        if i > 2:
            df_join[col] = df_join[col].str.replace('%', '').astype(float)
    
    col_pc, col_vc = 'Property Crime Rate', 'Violent Crime Rate'
    for dist in districts_to_collapse:
        cond1, cond2 = df_join['Community District'] == dist[0], df_join['Community District'] == dist[1]

        new_pc = (df_join.loc[cond1, col_pc].values[0] + df_join.loc[cond2, col_pc].values[0]) / 2
        new_vc = (df_join.loc[cond1, col_vc].values[0] + df_join.loc[cond2, col_vc].values[0]) / 2
        df_join.loc[cond1 | cond2, col_pc] = new_pc
        df_join.loc[cond1 | cond2, col_vc] = new_vc
    
    df_join['Community District'] = df_join['Community District'].map(districts_map)
    df_join.rename(columns={'Community District': 'Sub-Borough Area'})
    df_join = df_join.drop_duplicates()

    cols_to_drop = [str(year) for year in range(2000, 2021)]
    cols_to_drop.extend(['short_name', 'long_name'])

    for info in subboroughs_data_info:
        df = pd.read_csv(info[0])
        df.drop(columns=cols_to_drop, errors='ignore', inplace=True)
        df.rename(columns={'2021': info[1]}, inplace=True)
        df_join = pd.merge(df_join, df, on='Sub-Borough Area', how='inner')
    
    subboroughs_cols = [col[1] for col in subboroughs_data_info]
    for i, col in enumerate(subboroughs_cols):
        if i > 1:
            df_join[col] = (df_join[col].astype(float) * 100).round(2)
    
    df_join.to_csv('datasets/subboroughs-data.csv', index=False)



preprocess_subboroughs_data()
preprocess_servicerequests_data()

create_unique_incidents_dataset()
