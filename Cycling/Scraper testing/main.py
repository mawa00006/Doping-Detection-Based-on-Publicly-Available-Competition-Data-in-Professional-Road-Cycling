import numpy as np


from scraping_new import scrape_one_day_results
from scraping_new import scrape_rider_details
from scraping_new import scrape_races_for_year
from scraping_new import scrape_stage_race_all_stage_results
from scraping_new import scrape_stats_per_season

import pandas as pd
import time

#link to wiki page for rider

# racename, race year, race difficulty, rider name, age, gender, doping status, ranking, time, captain?, team, uci points, tour code, gc pos, distance, pcs_points, year
def main():
    start_time = time.time()

    df_stages, df_oneday, df_TTT, df_ITT = prepare_dataframes()

    details = scrape_rider_details('https://www.procyclingstats.com/rider/tadej-pogacar')


    # all years for to scrape available race data
    years = [2022]
    for year in years:

        # get all professional races held in a given year
        races = scrape_races_for_year(year)

        # for each race scrape all data
        for race in races.itertuples():

            # save basic information about the race in a dataframe
            # to concat it to each performance point later

            race_info_df = pd.DataFrame(race, index=[['index','stage_race', 'race_url', 'tour', 'tour_code']]).T
            race_info_df.drop(race_info_df.columns[[0,2]], axis= 1, inplace= True)


            # get race page url
            url = getattr(race, 'race_url')


            stage_race = getattr(race, 'stage_race')

            # scrape information and final result of each stage in a stage race
            if stage_race:
                stage_results = scrape_stage_race_all_stage_results(url+'/overview')

                # add race to each performance point
                for i in range(len(stage_results)):

                    stage_result = stage_results[i]

                    race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                    out_df = pd.concat([stage_result, race_inf], axis=1)

                    # append stage data to final output
                    df_stages = pd.concat([df_stages, out_df], axis=0)

            # scrape information and final result of a one-day race
            else:

                stage_results = scrape_one_day_results(url)
                df_oneday = pd.concat([df_oneday, stage_result], axis= 0)

    # key: rider_name,  value: pd.Dataframe containing rider details
    rider_detail_dict = {}
    rider_detail_df = pd.DataFrame(columns=["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                            "sprint_points", "climbing_points", "uci_world_ranking", ])
    stats_per_season_df = pd.DataFrame(columns=[])
    for stage_performance in df_stages.itertuples():
        rider = getattr(stage_performance, "rider_name")

        if rider in rider_detail_dict:
            details = rider_detail_dict[rider]
            rider_detail_df = pd.concat([rider_detail_df,details], axis = 1, ignore_index= True)

        else:
            details = scrape_rider_details()
            rider_detail_dict[rider] = details
            rider_detail_df = pd.concat([rider_detail_df, details], axis=1, ignore_index=True)
            stats_per_season = scrape_stats_per_season()
            stats_per_season_df = pd.concat([stats_per_season_df, stats_per_season], axis= 1, ignore_index= True)








    rider_detail_df = pd.DataFrame(columns=["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                            "sprint_points", "climbing_points", "uci_world_ranking", ])



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
                 'profile', 'distance', 'stage_pos', 'gc_pos',
                 'rider_age', 'team_name', 'rider_name',
                 'rider_nationality_code', 'uci_points', 'points', 'striked', 'race_name',
                 'stage_race', 'race_class', 'race_country_code'])

    # prepare dataframe for one day races
    df_oneday = pd.DataFrame(columns=[['stage_pos' 'bib_number' 'rider_age' 'team_name' 'rider_name', 'rider_nationality_code' 'uci_points' 'points' 'date' 'race_name', 'race_class' 'race_ranking' 'race_country_code' 'start_location', 'end_location' 'pcs_points_scale' 'profile' 'distance' 'vertical_meters', 'startlist_quality_score' 'striked']])

    # prepare dataframe for team time trials
    df_TTT = pd.DataFrame(columns=[])

    # prepare dataframe for individual time trials
    df_ITT = pd.DataFrame(columns=[])

    return df_stages, df_oneday, df_TTT, df_ITT





if __name__ == "__main__":
    main()

