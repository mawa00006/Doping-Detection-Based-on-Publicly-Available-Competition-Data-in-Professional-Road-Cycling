from scraping import scrape_rider_all_results
import pandas as pd
import numpy

#Wikipedia: 2013, 2014

def main():
    doping_list = ['lance-armstrong', 'michael-boogerd', 'eddy-bouwmans', 'thomas-dekker','jacky-durand', 'ryder-hesjedal',
                   'rudi-kemna', 'marc-lotz', 'stuart-o-grady', 'michael-rasmussen', 'rolf-sorensen', 'sylvain-georges',
                   'danilo-di-luca', 'mauro-santambrogio', 'alessandro-ballan', 'daryl-impey', 'patrik-sinkewitz',
                   'logan-loader', 'paolo-savoldelli', 'bart-voskamp', 'jonathan-tiernan-locke', 'carlos-barbero', 'denis-menchov',
                   'valentin-iglinskiy', 'matteo-rabottini', 'maxim-iglinskiy']
    # eine liste mit namen von dopern erstellen, sodass sie in die url eingefügt werden können (von wikipedia)
    # results für alle doper scrapen in einen df
    # spalte für namen hinzufügen
    # spalte für positives dopen hinzufügen

    #prepare dataframe
    df=pd.DataFrame(columns=["name", "date","type","result","gc_pos","race_country_code","race_name","race_class","stage_name","distance","pcs_points","uci_points","doped", ])

    #scrape all race results for the doped cyclists

    for doper in doping_list:

        #format url
        url = 'https://www.procyclingstats.com/rider/{}'.format(doper)

        #scrape results
        results = scrape_rider_all_results(url)

        # add name of cyclist to df
        entries =results.shape[0]
        print(entries)
        name_list = [doper]*entries
        results.insert(0, 'name', name_list)

        # remove url column
        results = results.drop('url',1)

        # add doping status of cyclist to df
        doped = [1]*entries
        results.insert(13, 'doped', doped)

        #apped to final dataframe
        df = df.append(results, ignore_index= True)

    # output dataframe as csv file
    df.to_csv('doped_cyclists_data.csv', index = True)

if __name__ == "__main__":
    main()

