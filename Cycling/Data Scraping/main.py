import os

from scraping import scrape_one_day_results
from scraping import scrape_rider_details
from scraping import scrape_races_for_year
from scraping import scrape_stage_race_all_stage_results
from scraping import scrape_stats_per_season

import pandas as pd
import numpy as np
import time
import csv


def main(years):
    #safe start time to calculate total runtime
    start_time = time.time()


    # key: rider_name,  value: pd.Dataframe containing rider details
    # Dictionary to save rider details, so they don't have to be scraped again for every performance point of a rider
    global rider_detail_dict
    global stage_dict

    rider_detail_dict = {}
    try:
        with open('Test-Data/riderdetaildicts/dict.csv', newline= '') as file:
            reader = csv.reader(file)
            next(reader)
            for rows in reader:
                name = rows[1]
                details = pd.DataFrame(rows[2].replace('[', '').replace( ']','').replace('\'','').split(' ')[1:11], index =["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                         "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"] ).T
                rider_detail_dict.update({name:details})
    except:
        rider_detail_dict = {}


    # scrape data for every race of a year
    for year in years:

        # get all professional races held in a given year
        races = scrape_races_for_year(year)

        # for each race scrape all data
        for race in races.itertuples():

            stage_dict = {}
            df_stages, df_oneday, stats_per_season_df = prepare_dataframes()
            # save basic information about the race in a dataframe
            # to concat it to each performance point later

            race_info_df = pd.DataFrame(race, index=[['index','stage_race', 'race_url', 'tour', 'tour_code']]).T
            race_info_df.drop(race_info_df.columns[[0,2]], axis= 1, inplace= True)

            # get race page url
            url = getattr(race, 'race_url')

            #stage race or one day race
            stage_race = getattr(race, 'stage_race')

            # scrape information and final result of each stage in a stage race
            if stage_race:

                try:
                    stage_results = scrape_stage_race_all_stage_results(url+'/overview')
                except:
                    continue

                # add race info to each performance point and concat all stage dataframes
                for i in range(len(stage_results)):

                    df_stages, df_oneday, stats_per_season_df = prepare_dataframes()

                    stage_result = stage_results[i]
                    if stage_result is None: continue

                    race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                    out_df = pd.concat([stage_result, race_inf], axis=1)

                    # append stage data to final output
                    df_stages = pd.concat([df_stages, out_df], axis=0)
                    # get rider details and stats per season
                    df_stages, stats_per_season_df = details_sps(df_stages, stats_per_season_df)
                    df_stages.columns = ['stage_pos', 'gc_pos', 'rider_age', 'team_name', 'rider_name',
                                         'rider_nationality_code', 'uci_points', 'points', 'striked', 'stage_name',
                                         'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code',
                                         'start_location', 'end_location', 'pcs_points_scale', 'profile', 'distance',
                                         'vertical_meters', 'startlist_quality_score', 'stage_race', 'tour',
                                         'tour_code',
                                         "DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                         "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"]
                    df_stages.to_csv('Test-Data/Stageraces/{}_{}_stage{}.csv'.format(df_stages.iloc[0]['race_name'], year,1), index=True)

                    exportdict(df_stages.iloc[0]['race_name'], year)
                    rider_detail_dict.update(stage_dict)
                    stats_per_season_df.to_csv('Test-Data/sps/sps_{}_{}.csv'.format(df_stages.iloc[0]['race_name'], year), index=True)

            # scrape information and final result of a one-day race
            else:
                try:
                    stage_result = scrape_one_day_results(url)
                except:
                    continue
                if stage_result is None: continue
                race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                out_df = pd.concat([stage_result, race_inf], axis=1)

                df_oneday = pd.concat([df_oneday, out_df], axis= 0)
                df_oneday, stats_per_season_df = details_sps(df_oneday, stats_per_season_df)

                df_oneday.columns = ['finish_pos', 'rider_age', 'team_name', 'rider_name', 'rider_nationality_code',
                                     'uci_points', 'points',
                                     'striked', 'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code',
                                     'start_location',
                                     'end_location', 'pcs_points_scale', 'profile', 'distance', 'vertical_meters',
                                     'startlist_quality_score',
                                     'stage_race', 'tour', 'tour_code', "DoB", "weight", "height", "one_day_points",
                                     "GC_points", "tt_points",
                                     "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"]
                df_oneday.to_csv('Test-Data/onedayraces/{}_{}.csv'.format(df_oneday.iloc[0]['race_name'], year), index=True)

                exportdict( df_oneday.iloc[0]['race_name'], year)
                rider_detail_dict.update(stage_dict)
                stats_per_season_df.to_csv('Test-Data/sps/sps_{}_{}.csv'.format(df_oneday.iloc[0]['race_name'], year), index=True)


    # print total runtime
    end_time = time.time()
    print('Runtime:', (end_time - start_time) / 60, 'min')

def exportdict(racename, year):
    w = csv.writer(open('Test-Data/riderdetaildicts/dict_{}_{}.csv'.format(racename, year), 'w'))
    for key, val in stage_dict.items():
        w.writerow([key,val.values])


def details_sps( df, stats_per_season_df):

    rider_detail_df = pd.DataFrame(columns=["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                            "sprint_points", "climbing_points", "uci_world_ranking",
                                            "all_time_ranking"])

    # append rider details and get stats per season
    for stage_performance in df.itertuples():
        rider = getattr(stage_performance, "rider_name")

        if rider in rider_detail_dict:
            print(rider, 'in dict')
            details = rider_detail_dict[rider]
            rider_detail_df = pd.concat([rider_detail_df, details], axis=0, ignore_index=True)

        else:
            print(rider)
            try: details = scrape_rider_details('https://www.procyclingstats.com/rider/{}'.format(rider))
            except: details = pd.DataFrame([np.NaN]*10).T
            stage_dict[rider] = details
            rider_detail_df = pd.concat([rider_detail_df, details], axis=0, ignore_index=True)

            try: stats_per_season = scrape_stats_per_season(
                'https://www.procyclingstats.com/rider/{}/statistics/overview'.format(rider), rider)
            except: stats_per_season = pd.DataFrame([np.NaN]*4).T
            stats_per_season_df = pd.concat([stats_per_season_df, stats_per_season], axis=0, ignore_index=True)

    if df.shape[0] == rider_detail_df.shape[0]:
        df.reset_index(drop=True, inplace=True)
        rider_detail_df.reset_index(drop=True, inplace=True)
        df = pd.concat([df, rider_detail_df], axis = 1, ignore_index= True)


    return  df, stats_per_season_df




def prepare_dataframes():

    # prepare dataframe for stage races
    df_stages = pd.DataFrame(
        columns=['stage_pos', 'gc_pos', 'rider_age', 'team_name', 'rider_name',
                'rider_nationality_code', 'uci_points', 'points', 'striked', 'stage_name',
                'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code',
                'start_location', 'end_location', 'pcs_points_scale', 'profile', 'distance',
                'vertical_meters', 'startlist_quality_score'])

    # prepare dataframe for one day races
    df_oneday = pd.DataFrame(
        columns=['finish_pos', 'rider_age', 'team_name', 'rider_name', 'rider_nationality_code', 'uci_points', 'points',
                 'striked', 'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code', 'start_location',
                 'end_location', 'pcs_points_scale', 'profile', 'distance', 'vertical_meters','startlist_quality_score'])

    # prepare dataframe to hold stats per seasons
    stats_per_season_df = pd.DataFrame(columns=["name", "season", "points", "wins", "racedays"])

    return df_stages, df_oneday, stats_per_season_df


if __name__ == "__main__":
    # all years for which to scrape available race data
    years = np.arange(2000, 2023, 1)

    for file in os.listdir("Test-Data/riderdetaildicts/"):
            if file.endswith('.csv'):
                if file == 'dict.csv':
                    continue
                try: tmp = pd.read_csv(os.path.join("Test-Data/riderdetaildicts/", file))
                except: continue
                tmp.to_csv('Test-Data/riderdetaildicts/dict.csv', mode = 'a')

    main(years)



