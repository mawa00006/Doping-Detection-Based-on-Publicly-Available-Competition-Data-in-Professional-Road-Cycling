import numpy as np

from scraping import get_rider_teams
from scraping import get_rider_years
from scraping import get_rider_details
from scraping import scrape_riders_from_team
from scraping import scrape_one_day_results
from scraping import scrape_race_startlist
from scraping import scrape_stage_race_stage_results
from scraping import scrape_race_information
from scraping import scrape_races_for_year
from scraping import scrape_stage_race_all_stage_results
from scraping import scrape_stage_race_overview_competing_teams
from scraping import scrape_stage_race_overview_stages
from scraping import scrape_stage_race_overview_top_competitors
from scraping import scrape_tour_races_for_year
from scraping import scrape_teams_for_year
from scraping import scrape_rider_all_results

import pandas as pd
import time

#link to wiki page for rider

# racename, race year, race difficulty, rider name, age, gender, doping status, ranking, time, captain?, team, uci points, tour code, gc pos, distance, pcs_points, year
def main():
    start_time = time.time()

    df_stages, df_oneday, df_TTT, df_ITT = prepare_dataframes()


    # all years from which to scrape available race data
    years = [2014]
    for year in years:

        # get all professional races held in a given year
        races = scrape_races_for_year(year)
        # for each race scrape all data
        for race in races.itertuples():

            # save basic information about the race in a dataframe
            # to concat it to each performance point later
            stage_race = getattr(race, 'stage_race')

            race_info_df = pd.DataFrame(race).T
            race_info_df = race_info_df.drop(race_info_df.columns[[0,1, 7]], axis = 1)
            race_info_df.columns = ["race_name","stage_race","race_class","race_country_code",
                                    "cancelled","tour", 'tour_code']

            # get race page url
            url = getattr(race, 'race_url')
            # scrape information and final result of each stage in a stage race
            if stage_race:
                stage_results, stages = scrape_stage_race_all_stage_results(url+'/overview', collecting =1)

            # scrape information and final result of a one-day race
            else:
                pass
                #stage_results = scrape_one_day_results(url)
                pass
                    #TODO

            # add race/stage information to each performance point
            for i in range(len(stage_results)):

                stage_result = stage_results[i]
                stage = stages.iloc[[i]]
                stage_info_df = pd.DataFrame(stage)
                url = stage_info_df['stage_url'].loc[stage_info_df.index[0]]
                if pd.isna(url): continue
                else: stage_inf = scrape_race_information(url)
                stage_info_df = pd.concat([stage_inf, stage_info_df.reset_index()],axis = 1)
                stage_info_df = pd.concat([stage_info_df]*stage_result.shape[0], ignore_index= True)
                out_df = pd.concat([stage_info_df, stage_result], axis = 1)
                race_inf  = pd.concat([race_info_df]*stage_result.shape[0], ignore_index= True)
                out_df = pd.concat([out_df, race_inf], axis = 1)

                # append data to final output
                df_stages = pd.concat([df_stages, out_df], axis = 0)


    df_stages = df_stages[['striked','race_name','date','rider_name', 'stage_pos', 'gc_pos',
                 'rider_nationality_code', 'rider_age', 'team_name', 'distance','profile_score','profile', 'vertical_meters',
                 'startlist_quality_score', 'stage_name', 'start_location', 'end_location',  'stage_url',
                 'bib_number',  'uci_points', 'points', 'stage_race', 'race_class', 'race_country_code', 'cancelled',
                 'tour', 'tour_code']]

    end_time = time.time()


    print('Runtime:', end_time - start_time)

    df_stages.to_csv('Test-Data/test.csv', index= True)


def prepare_dataframes():

    # prepare dataframe fro stage races
    df_stages = pd.DataFrame(
        columns=['date', 'profile_score', 'vertical_meters', 'startlist_quality_score', 'stage_name', 'start_location',
                 'end_location',
                 'profile', 'distance', 'stage_url', 'stage_pos', 'gc_pos',
                 'bib_number', 'rider_age', 'team_name', 'rider_name',
                 'rider_nationality_code', 'uci_points', 'points', 'striked', 'race_name',
                 'stage_race', 'race_class', 'race_country_code', 'cancelled', 'tour', 'tour_code'])

    # prepare dataframe for one day races
    df_oneday = pd.DataFrame(columns=[])

    # prepare dataframe for team time trials
    df_TTT = pd.DataFrame(columns=[])

    # prepare dataframe for individual time trials
    df_ITT = pd.DataFrame(columns=[])

    return df_stages, df_oneday, df_TTT, df_ITT





if __name__ == "__main__":
    main()

def testedfunctions():
    races_2014 = scrape_races_for_year(2014)

    races_2014.to_csv('Test-Data/races_2014_test.csv', index = True)

    tour_races_for_year_2014 = scrape_tour_races_for_year(2014)
    tour_races_for_year_2014.to_csv('Test-Data/tour_races_for_year_2014.csv', index = True)

    df = pd.DataFrame(
        columns=["stage_pos", "gc_pos", "bib_number", "rider_age", "team_name", "rider_name", "rider_nationality_code",
                 "uci_points", "points"])

    results, stages = scrape_stage_race_all_stage_results('https://www.procyclingstats.com/race/tour-de-france/2020/overview', collecting=1)
    for stage in results:
        df = pd.concat([df, stage])

    df.to_csv('Test-Data/tdf2020_results_test.csv', index=True)




    for stage in stages.itertuples(index = False):
        if getattr(stage, 'stage_name') == "REST DAY":
            continue
        stage_url = getattr(stage, 'stage_url')
        stage_results_df = scrape_stage_race_stage_results(stage_url)
        stage_df = pd.DataFrame(stage).T
        stage_df = pd.concat([stage_df]*stage_results_df.shape[0], ignore_index= True)
        stage_results_df = pd.concat([stage_results_df, stage_df], axis = 1)