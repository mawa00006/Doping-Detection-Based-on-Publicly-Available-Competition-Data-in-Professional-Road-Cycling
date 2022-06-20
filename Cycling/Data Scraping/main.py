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

    global rider_detail_dict

    #extract rider_detail_dict from dict.csv file
    #this will load all rider details that where scraped previously back into the rider_detail_dict

    #rider_dict_from_csv()  #uncomment this line if the script crashed at any point (E.G.: network connection failed)


    # scrape data for every race of a year
    for year in years:
        print(year)

        # get all professional races held in a given year
        races = scrape_races_for_year(year)


        # for each race scrape all data
        for race in races.itertuples():

            #dictionary to safe rider details of each (stage)
            global stage_dict
            stage_dict = {}

            #reset datfarames for each race
            df_stages, df_oneday, stats_per_season_df = prepare_dataframes()

            # save basic information about the race in a dataframe
            # to concat it to each performance point later
            race_info_df = pd.DataFrame(race, index=[['index','stage_race', 'race_url', 'tour', 'tour_code']]).T
            race_info_df.drop(race_info_df.columns[[0,2]], axis= 1, inplace= True)

            # get race page url
            url = getattr(race, 'race_url')


            # check if race was already scraped
            # if url in race_url_dict: continue #uncomment this line if the script crashed at any point (E.G.: network connection failed)

            race_url_dict[url] = np.NaN

            #stage race or one day race
            stage_race = getattr(race, 'stage_race')

            # scrape information and final result of each stage in a stage race
            if stage_race:
                try:
                    try:
                        #scrape results of each stage in a stage race
                        stage_results = scrape_stage_race_all_stage_results(url+'/overview')
                    except Exception as e:
                        print('Exception occured while scraping all stage results', e, getattr(race, 'race_url'))
                        continue

                    # add race info to each performance point and concat all stage dataframes
                    for i in range(len(stage_results)):

                        # reset dataframes for each stage in a stage race
                        df_stages, df_oneday, stats_per_season_df = prepare_dataframes()

                        stage_result = stage_results[i]
                        if stage_result is None:
                            print(url, 'stage', i, 'is none')
                            continue

                        #add race information
                        race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                        out_df = pd.concat([stage_result, race_inf], axis=1)

                        # append stage data to final output
                        df_stages = pd.concat([df_stages, out_df], axis=0)
                        # get rider details and stats per season
                        df_stages, stats_per_season_df = details_sps(df_stages, stats_per_season_df)
                        # set column names
                        df_stages = set_column_names(df_stages)

                        # save scraped stage data as a csv files
                        df_stages.to_csv('Test-Data/Stageraces/{}_{}_stage{}.csv'.format(df_stages.iloc[0]['race_name'], year,i), index=True)
                        exportdict(df_stages.iloc[0]['race_name'], year)
                        stats_per_season_df.to_csv(
                            'Test-Data/sps/sps_{}_{}_stage{}.csv'.format(df_stages.iloc[0]['race_name'], year, i), index=True)

                        #update rider_detail_dict
                        rider_detail_dict.update(stage_dict)

                except Exception as e:
                    print('Exception occured while merging stage results', e, getattr(race, 'stage_url'))
                    continue

            # scrape information and final result of a one-day race
            else:
                try:
                    try:
                        # scrape results of a oneday race
                        stage_result = scrape_one_day_results(url)
                    except Exception as e:
                        print('Exception occured while scraping onedayresults', e, getattr(race_info_df, 'stage_url'))
                        continue

                    if stage_result is None:
                        print('odr: race is none', url)
                        continue
                    # add race info to each performance point
                    race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                    out_df = pd.concat([stage_result, race_inf], axis=1)
                    df_oneday = pd.concat([df_oneday, out_df], axis= 0)

                    # get rider details and stats per season
                    df_oneday, stats_per_season_df = details_sps(df_oneday, stats_per_season_df)

                    # set column names
                    df_oneday = set_column_names(df_oneday, False)

                    # save scraped race data as a csv files
                    df_oneday.to_csv('Test-Data/onedayraces/{}_{}.csv'.format(df_oneday.iloc[0]['race_name'], year), index=True)
                    stats_per_season_df.to_csv('Test-Data/sps/sps_{}_{}.csv'.format(df_oneday.iloc[0]['race_name'], year), index=True)
                    exportdict( df_oneday.iloc[0]['race_name'], year)

                    #update rider_detail_dict
                    rider_detail_dict.update(stage_dict)


                except Exception as e:
                    print('Exception while merging table in onedayresults:', e)
                    continue

    return True

def set_column_names(df, stagerace = True):
    '''
    SUMMARY
    Set comlumn names for either stage race or onedayrace dataframe
    PARAMETERS
    df (pd.Dataframe): Dataframe to rename
    stagerace (Boolean): indicating if stage race or onedayrace True = stage, False = oneday
    OUTPUT
    pd.Dataframe: renamed dataframe
    '''

    if stagerace:
        df.columns = ['stage_pos', 'gc_pos', 'rider_age', 'team_name', 'rider_name',
                             'rider_nationality_code', 'uci_points', 'points', 'striked', 'stage_name',
                             'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code',
                             'start_location', 'end_location', 'pcs_points_scale', 'profile', 'distance',
                             'vertical_meters', 'startlist_quality_score', 'stage_race', 'tour',
                             'tour_code',
                             "DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                             "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"]
    else:
        df.columns = ['finish_pos', 'rider_age', 'team_name', 'rider_name', 'rider_nationality_code',
                             'uci_points', 'points',
                             'striked', 'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code',
                             'start_location',
                             'end_location', 'pcs_points_scale', 'profile', 'distance', 'vertical_meters',
                             'startlist_quality_score',
                             'stage_race', 'tour', 'tour_code', "DoB", "weight", "height", "one_day_points",
                             "GC_points", "tt_points",
                             "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"]

    return df





def rider_dict_from_csv():
    '''
    SUMMARY
    If the script crashes we do not want to scrape everything again thus we need to get previously scraped
    rider information back into the programm. This is achieved by concatenating all riderdetaildicts that
    have already been scraped and then adding the information back into the rider_detail_dict

    '''
    global rider_detail_dict

    for file in os.listdir("Test-Data/riderdetaildicts/"):
            if file.endswith('.csv'):
                if file == 'dict.csv':
                    continue
                try: tmp = pd.read_csv(os.path.join("Test-Data/riderdetaildicts/", file))
                except: continue
                tmp.to_csv('Test-Data/riderdetaildicts/dict.csv', mode = 'a')

    with open('Test-Data/riderdetaildicts/dict.csv', newline='') as file:
            reader = csv.reader(file)
            next(reader)
            for rows in reader:
                name = rows[1]
                try:
                    details = pd.DataFrame(rows[2].replace('[', '').replace( ']','').replace('\'','').split(' ')[1:11], index =["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                        "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"] ).T
                except:
                    details = pd.DataFrame([np.NaN]*10,index =["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                        "sprint_points", "climbing_points", "uci_world_ranking", "all_time_ranking"] ).T

                rider_detail_dict[name]= details




def exportdict(racename, year):
    '''
    SUMMARY
    save scraped rider information for a stage/race as .csv file
    '''
    w = csv.writer(open('Test-Data/riderdetaildicts/dict_{}_{}.csv'.format(racename, year), 'w'))
    for key, val in stage_dict.items():
        w.writerow([key,val.values])


def details_sps(df, stats_per_season_df):
    '''
    SUMMARY
    Create a dataframe containing rider details and stats per season for each rider in a race
    Save rider details into rider_detail_dict
    PARAMETERS
    df (pd.Dataframe): Dataframe containing race results
    stats_per_season_df (pd.Dataframe): Empty dataframe to fill with stats per season for each new rider
    OUTPUT
    df (pd.Dataframe): Datframe containing race results and rider details
    stats_per_season_df (pd.Dataframe): Dataframe containing stats per season for each new rider
    '''

    rider_detail_df = pd.DataFrame(columns=["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                            "sprint_points", "climbing_points", "uci_world_ranking",
                                            "all_time_ranking"])

    # append rider details and get stats per season
    for stage_performance in df.itertuples():
        rider = getattr(stage_performance, "rider_name")

        if rider in rider_detail_dict:
            details = rider_detail_dict[rider]
            rider_detail_df = pd.concat([rider_detail_df, details], axis=0, ignore_index=True)

        else:
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
    years = np.arange(2016, 2023, 1)

    # key: rider_name,  value: pd.Dataframe containing rider details
    # Dictionary to save rider details, so they don't have to be scraped again for every performance point of a rider
    global rider_detail_dict
    global race_url_dict
    race_url_dict = {}
    rider_detail_dict = {}

    #run script until all races are scraped. prevent the script from stopping, even if an internal error occures
    while(1):
        try:
           if main(years):
               break
           else:
               continue
        except Exception as e:
            print(e)
            continue

    # concat all scraped riderdict/sps files and save them as .csv files
    for file in os.listdir("Test-Data/riderdetaildicts/"):
            if file.endswith('.csv'):
                if file == 'dict.csv':
                    continue
                try: tmp = pd.read_csv(os.path.join("Test-Data/riderdetaildicts/", file))
                except: continue
                tmp.to_csv('Test-Data/riderdetaildicts/dict.csv', mode = 'a')


    for file in os.listdir("Test-Data/sps/"):
            if file.endswith('.csv'):
                try: tmp = pd.read_csv(os.path.join("Test-Data/sps/", file))
                except: continue
                tmp.to_csv('Test-Data/sps/final_sps.csv', mode = 'a')




