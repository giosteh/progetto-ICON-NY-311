# imports
import pandas as pd
from datetime import timedelta
from geopy.distance import great_circle


PROJECT_PATH = 'progetto-ICON-NY-311/src/datasets/'

# dizionario che mappa i nomi dei borough ai loro acronimi
ACRONYMS = {'BRONX': 'BX',
            'BROOKLYN': 'BK',
            'MANHATTAN': 'MN',
            'QUEENS': 'QN',
            'STATEN ISLAND': 'SI'}

# dizionario che mappa i codici dei distretti alle sub-borough area
DISTRICTS_MAP = {'BK01': 'Williamsburg/Greenpoint',
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
    df = pd.read_csv(PROJECT_PATH + '311-2023-05.csv')

    cols_to_drop = ['Street Name', 'Vehicle Type', 'Taxi Company Borough']
    cols_to_normalize = ['Complaint Type', 'Descriptor', 'Location Type',
                         'Incident Address', 'Street Name', 'Status',
                         'Borough', 'Open Data Channel Type']
    
    normalize_string_spaces = lambda s: ' '.join(s.split())
    def transform_district_codes(s):
        number, borough = s.split(' ', 1)
        return f"{ACRONYMS.get(borough)}{number}"
    
    df.drop(columns=cols_to_drop, inplace=True)
    df.rename(columns={'Community Board': 'Sub-Borough Area'}, inplace=True)

    df = df.dropna(subset=['Location'])
    for col in df.columns:
        df[col] = df[col].fillna('unknown')
    
    for col in cols_to_normalize:
        df[col] = df[col].apply(normalize_string_spaces)
        df[col] = df[col].str.lower()
    
    df['Sub-Borough Area'] = df['Sub-Borough Area'].apply(transform_district_codes).map(DISTRICTS_MAP)

    df[['Latitude', 'Longitude']] = df['Location'].str.extract(r'\(([^,]+), ([^,]+)\)').astype(float)
    df.drop(columns=['Location'], inplace=True)

    df.to_csv(PROJECT_PATH + '311-2023-05-v2.csv', index=False)


def create_unique_incidents_dataset():
    """
    Individua, a partire dal dataset preprocessato '311-2023-05-v2.csv',
    gli `incident` unici oggetto di più richieste di servizio,
    isolandoli e realizzando un dataset che li collezioni.
    """
    df = pd.read_csv(PROJECT_PATH + '311-2023-05-v2.csv')
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
                        'Location Type': row['Location Type'],
                        'Address': row['Incident Address'],
                        'Borough': row['Borough'],
                        'Sub-Borough Area': row['Sub-Borough Area']}
        incidents_rows.append(incident_row)
    
    df_incidents = pd.DataFrame(incidents_rows)
    df_incidents.to_csv(PROJECT_PATH + '311-unique-incidents.csv', index=False)

    df.drop(columns=['Complaint Type', 'Descriptor', 'Location Type',
                     'Borough', 'Sub-Borough Area', 'Incident Address'], inplace=True)
    df.to_csv(PROJECT_PATH + '311-2023-05-v3.csv', index=False)
    

# lista dei path dei dataset sui distretti
DISTRICTS_DATA_PATHS = ['district-incomedistribution.csv',
                        'district-povertyrate.csv',
                        'district-racecomposition.csv',
                        'district-crimerate.csv']

# lista delle coppie di distretti i cui dati sono ripetuti
DISTRICTS_TO_COLLAPSE = [('BX01', 'BX02'),
                         ('BX03', 'BX06'),
                         ('MN01', 'MN02'),
                         ('MN04', 'MN05')]

# lista delle coppie (path, rinominazione della colonna 2021) per ciascuno dei dataset sui sub-borough
SUBBOROUGHS_DATA_INFO = [('sub-borougharea-populationdensity1000personspersquaremile.csv', 'Population Density'),
                         ('sub-borougharea-populationaged65.csv', 'Population Aged 65+'),
                         ('sub-borougharea-borninnewyorkstate.csv', 'NYS Born People'),
                         ('sub-borougharea-foreign-bornpopulation.csv', 'Foreign Born People'),
                         ('sub-borougharea-disabledpopulation.csv', 'Disabled People'),
                         ('sub-borougharea-unemploymentrate.csv', 'Unemployment Rate'),
                         ('sub-borougharea-car-freecommuteofcommuters.csv', 'Car-Free Commuters'),
                         ('sub-borougharea-householdswithchildrenunder18yearsold.csv', 'Families with Children'),
                         ('sub-borougharea-populationaged25withabachelorsdegreeorhigher.csv', 'People O25 with Bachelor'),
                         ('sub-borougharea-populationaged25withoutahighschooldiploma.csv', 'People O25 without Diploma')]


def preprocess_subboroughs_data():
    """
    Esegue il preprocessing dei dataset sulle sub-borough area,
    ognuno contenente uno o più attributi di interesse. I dataset
    sono poi joinati tra loro per ottenere un unico dataset sui sub-boroughs.
    """
    df_income_distribution = pd.read_csv(PROJECT_PATH + DISTRICTS_DATA_PATHS[0])
    df_poverty_rate = pd.read_csv(PROJECT_PATH + DISTRICTS_DATA_PATHS[1])
    df_race_composition = pd.read_csv(PROJECT_PATH + DISTRICTS_DATA_PATHS[2])
    df_crime_rate = pd.read_csv(PROJECT_PATH + DISTRICTS_DATA_PATHS[3])
    
    df_crime_rate.rename(columns={'year': 'Year'}, inplace=True)
    df_crime_rate = df_crime_rate[df_crime_rate['Year'] == '2018-2022']
    
    def process_districts_data(df, cols_to_drop, rename_columns):
        df = df.iloc[6:]
        df.drop(columns=cols_to_drop, inplace=True)
        df.rename(columns={'Geography': 'Community District'}, inplace=True)
        df['Community District'] = df['Community District'].apply(lambda s: ''.join(s.split()))
        df.rename(columns=rename_columns, inplace=True)
        return df
    
    cols_to_drop = ['Name', 'Level', 'Year']
    df_crime_rate = process_districts_data(df_crime_rate, cols_to_drop, {'property_crime_rate': 'Property Crime Rate',
																'violent_crime_rate': 'Violent Crime Rate'})
    df_income_distribution = process_districts_data(df_income_distribution, cols_to_drop, {'<= $20,000': 'Low Income Population',
																				  '$20,001 -\n$40,000': 'Medium-Low Income Population',
																				  '$40,001 -\n$60,000': 'Medium Income Population',
																				  '$60,001 -\n$100,000': 'Medium-High Income Population',
																				  '$100,001 -\n$250,000': 'High Income Population',
																				  '> $250,000': 'Very High Income Population'})
    df_poverty_rate = process_districts_data(df_poverty_rate, cols_to_drop, {'poverty_rate': 'Poverty Rate'})
    df_race_composition = process_districts_data(df_race_composition, cols_to_drop, {'pop_hispanic_pct': 'Hispanic Population',
																			'pop_non_hispanic_asian_pct': 'Asian Population',
																			'pop_non_hispanic_black_pct': 'Black Population',
																			'pop_non_hispanic_white_pct': 'White Population'})
    
    df_join = pd.merge(df_crime_rate, df_income_distribution, on='Community District', how='inner')
    df_join = pd.merge(df_join, df_poverty_rate, on='Community District', how='inner')
    df_join = pd.merge(df_join, df_race_composition, on='Community District', how='inner')

    for i, col in enumerate(df_join.columns):
        if i > 2:
            df_join[col] = df_join[col].str.replace('%', '').astype(float)
    
    col_pc, col_vc = 'Property Crime Rate', 'Violent Crime Rate'
    for dist in DISTRICTS_TO_COLLAPSE:
        cond1, cond2 = df_join['Community District'] == dist[0], df_join['Community District'] == dist[1]

        new_pc = (df_join.loc[cond1, col_pc].values[0] + df_join.loc[cond2, col_pc].values[0]) / 2
        new_vc = (df_join.loc[cond1, col_vc].values[0] + df_join.loc[cond2, col_vc].values[0]) / 2
        df_join.loc[cond1 | cond2, col_pc] = new_pc
        df_join.loc[cond1 | cond2, col_vc] = new_vc
    
    df_join['Community District'] = df_join['Community District'].map(DISTRICTS_MAP)
    df_join.rename(columns={'Community District': 'Sub-Borough Area'})
    df_join = df_join.drop_duplicates()

    cols_to_drop = [str(year) for year in range(2000, 2021)]
    cols_to_drop.extend(['short_name', 'long_name'])

    for info in SUBBOROUGHS_DATA_INFO:
        df = pd.read_csv(PROJECT_PATH + info[0])
        df.drop(columns=cols_to_drop, errors='ignore', inplace=True)
        df.rename(columns={'2021': info[1]}, inplace=True)
        df_join = pd.merge(df_join, df, on='Sub-Borough Area', how='inner')
    
    subboroughs_cols = [col[1] for col in SUBBOROUGHS_DATA_INFO]
    for i, col in enumerate(subboroughs_cols):
        if i > 1:
            df_join[col] = (df_join[col].astype(float) * 100).round(2)
    
    df_join.to_csv(PROJECT_PATH + 'subboroughs-ny.csv', index=False)



preprocess_subboroughs_data()
preprocess_servicerequests_data()

create_unique_incidents_dataset()
