from bs4 import BeautifulSoup
from scraping import scrape_dopeology_incidents
from scraping import scrape_incident
from requests_html import HTMLSession # to remove
import pandas as pd
import numpy as np


def scrape_adrv():

    incidents = scrape_dopeology_incidents()

    df = pd.DataFrame()

    for incident in incidents.itertuples():

        url = getattr(incident, 'incident_url')

        print(url)

        people = scrape_incident(url)
        if people is None:
            continue
        incident = pd.DataFrame([incident])
        incident = pd.concat([incident]*people.shape[0], axis =0)

        incident_df = pd.concat([people, incident], axis =1, ignore_index= True)
        df = pd.concat([df, incident_df], axis = 0)

    df.rename(columns=['name', 'team', 'role', 'to_drop', 'incident_name', 'incident_url',
                       'incident_type', 'incident_date'])

    df.to_csv('dopeology.csv')
    return df


scrape_adrv()