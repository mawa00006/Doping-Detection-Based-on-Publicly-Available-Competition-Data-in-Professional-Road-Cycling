from bs4 import BeautifulSoup
from scraping import scrape_dopeology_incidents
from scraping import scrape_incident
import os
import pandas as pd
import numpy as np


def scrape_adrv():

    incidents = scrape_dopeology_incidents().tail(1314-1100)

    df = pd.DataFrame()

    i= 1101
    for incident in incidents.itertuples():
        try:
            print(i)
            i+= 1

            url = getattr(incident, 'incident_url')

            print(url)

            people = scrape_incident(url)
            if people is None:
                continue
            incident = pd.DataFrame([incident])
            incident = pd.concat([incident]*people.shape[0], axis =0)

            incident_df = pd.concat([people, incident], axis =1, ignore_index= True)
            df = pd.concat([df, incident_df], axis = 0)
        except Exception as e:
            print(e)
            continue

        if i % 100 == 0:
            df.to_csv('dopeology_{}.csv'.format(i))


    df.to_csv('dopeology.csv')
    return df


#scrape_adrv()


