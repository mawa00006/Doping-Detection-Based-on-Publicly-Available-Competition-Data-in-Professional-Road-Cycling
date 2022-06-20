
from scraping import scrape_dopeology_incidents
from scraping import scrape_incident
import pandas as pd



def scrape_adrv():

    # scrape all incidents from dopeology.com
    incidents = scrape_dopeology_incidents()

    df = pd.DataFrame()

    # scrape information of each incident
    for incident in incidents.itertuples():
        try:


            url = getattr(incident, 'incident_url')
            print(url)


            people = scrape_incident(url)
            if people is None:
                continue

            # save incident details into dataframe
            incident = pd.DataFrame([incident])
            incident = pd.concat([incident] * people.shape[0], axis=0)

            # add incident to final df
            incident_df = pd.concat([people, incident], axis =1, ignore_index= True)
            df = pd.concat([df, incident_df], axis = 0)

        except Exception as e:
            print(e)
            continue

    # save scraped data as .csv file
    df.to_csv('dopeology.csv')

    return df


scrape_adrv()


