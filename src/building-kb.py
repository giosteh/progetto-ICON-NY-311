# imports
import pandas as pd
from pyswip import Prolog


def write_fact(path, fact):
    """
    Scrive su file la stringa passata.
    """
    with open('path', 'w') as file:
        file.write(fact)


def create_facts_from_datasets():
    """
    """
    df_requests = pd.read_csv('datasets/311-2023-05-v3.csv')
    df_incidents = pd.read_csv('datasets/311-unique-incidents.csv')
    df_subboroughs = pd.read_csv('datasets/subboroughs-ny.csv')


    write_fact('facts.pl', ':-style_check(-discontiguous).\n')

    for i, row in df_requests.iterrows():
        request_id = f"request({row['Unique Key']})"
        fact = (f"created_date({request_id}, {row['Created Date']}).\n"
                f"closed_date({request_id}, {row['Closed Date']}).\n"
                f"channel_type({request_id}, {row['Open Data Channel Type']}).\n"
                f"latitude({request_id}, {row['Latitude']}).\n"
                f"longitude({request_id}, {row['Longitude']}).\n"
                f"report({request_id}, {row['Incident Id']}).\n")
        write_fact('facts.pl', fact)

    for i, row in df_incidents.iterrows():
        incident_id = f"incident({row['Incident Id']})"
        fact = (f"earliest_created_date({incident_id}, {row['Earliest Created Date']}).\n"
                f"complaint_type({incident_id}, {row['Complaint Type']}).\n"
                f"location_type({incident_id}, {row['Location Type']}).\n"
                f"address({incident_id}, {row['Address']}).\n"
                f"borough({incident_id}, {row['Borough']}).\n"
                f"subborough({incident_id}, {row['Sub-Borough Area']}).\n")
        write_fact('facts.pl', fact)
    
    for i, row in df_subboroughs.iterrows():
        subborough_id = f"subborough({row['Sub-Borough Area']})"
        fact = (f"property_crime_rate({subborough_id}, {row['Property Crime Rate']}).\n"
                f"violent_crime_rate({subborough_id}, {row['Violent Crime Rate']}).\n"
                f"low_income_pop({subborough_id}, {row['Low Income Population']}).\n"
                f"mediumlow_income_pop({subborough_id}, {row['Medium-Low Income Population']}).\n"
                f"medium_income_pop({subborough_id}, {row['Medium Income Population']}).\n"
                f"mediumhigh_income_pop({subborough_id}, {row['Medium-High Income Population']}).\n"
                f"high_income_pop({subborough_id}, {row['High Income Population']}).\n"
                f"veryhigh_income_pop({subborough_id}, {row['Very High Income Population']}).\n"
                f"poverty_rate({subborough_id}, {row['Poverty Rate']}).\n"
                f"hispanic_pop({subborough_id}, {row['Hispanic Population']}).\n"
                f"asian_pop({subborough_id}, {row['Asian Population']}).\n"
                f"black_pop({subborough_id}, {row['Black Population']}).\n"
                f"white_pop({subborough_id}, {row['White Population']}).\n"
                f"pop_density({subborough_id}, {row['Population Density']}).\n"
                f"pop_aged_65({subborough_id}, {row['Population Aged 65+']}).\n"
                f"nys_born_people({subborough_id}, {row['NYS Born People']}).\n"
                f"foreign_born_people({subborough_id}, {row['Foreign Born People']}).\n"
                f"disabled_people({subborough_id}, {row['Disabled People']}).\n"
                f"unemployment_rate({subborough_id}, {row['Unemployment Rate']}).\n"
                f"carfree_commuters({subborough_id}, {row['Car-Free Commuters']}).\n"
                f"families_with_children({subborough_id}, {row['Families with Children']}).\n"
                f"people_with_bachelor({subborough_id}, {row['People O25 with Bachelor']}).\n"
                f"people_without_diploma({subborough_id}, {row['People O25 without Diploma']}).\n")
        write_fact('facts.pl', fact)

