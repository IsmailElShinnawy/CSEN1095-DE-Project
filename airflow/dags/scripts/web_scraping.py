import pandas as pd

URL = 'https://www.citypopulation.de/en/uk/admin/'

def get_population_data():
    return pd.read_html(URL)[0]

def get_population_estimate(row):
    return round(row['PopulationCensus2021-03-21']-row['PopulationEstimate2011-06-30']*0.8)

def augment_df_with_population_data(df):
    population_df = get_population_data()
    population_df['population_2019'] = population_df.apply(get_population_estimate, axis=1)
    
    district_pop = dict()
    for i in range(len(population_df)):
        if(population_df.iloc[i]['Name'] not in district_pop.keys()):
            district_pop[population_df.iloc[i]['Name']] = population_df.iloc[i]['population_2019']
    
    df['district_population'] = df.apply(lambda _: 'nan', axis=1)
    for i in range(len(df)):
        if(df.iloc[i]['local_authority_district'] in district_pop.keys()):
            df.at[df.index[i],'district_population'] = district_pop.get(df.iloc[i]['local_authority_district'])