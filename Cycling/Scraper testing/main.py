
from scraping_new import scrape_one_day_results
from scraping_new import scrape_rider_details
from scraping_new import scrape_races_for_year
from scraping_new import scrape_stage_race_all_stage_results
from scraping_new import scrape_stats_per_season

import pandas as pd
import time


def main():
    #safe start time to calculate total runtime
    start_time = time.time()

    #prepare datframes
    df_stages, df_oneday, stats_per_season_df = prepare_dataframes()


    # all years for to scrape available race data
    years = [2022]

    # scrape data for every race of a year
    for year in years:

        # get all professional races held in a given year
        races = scrape_races_for_year(year)

        races = races.head()

        # for each race scrape all data
        for race in races.itertuples():

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

                    stage_result = stage_results[i]

                    race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                    out_df = pd.concat([stage_result, race_inf], axis=1)

                    # append stage data to final output
                    df_stages = pd.concat([df_stages, out_df], axis=0)

            # scrape information and final result of a one-day race
            else:
                try:
                    stage_result = scrape_one_day_results(url)
                except:
                    continue
                race_inf = pd.concat([race_info_df] * stage_result.shape[0], ignore_index=True)
                out_df = pd.concat([stage_result, race_inf], axis=1)

                df_oneday = pd.concat([df_oneday, out_df], axis= 0)


    # key: rider_name,  value: pd.Dataframe containing rider details
    # Dictionary to save rider details, so they don't have to be scraped again for every performance point of a rider
    rider_detail_dict = {}

    #prepare dataframe to hold stats per seasons
    stats_per_season_df = pd.DataFrame(columns=["name", "season", "points", "wins", "racedays"])

    rider_detail_dict, df_stages, stats_per_season_df = details_sps(rider_detail_dict, df_stages, stats_per_season_df)

    rider_detail_dict, df_oneday, stats_per_season_df = details_sps(rider_detail_dict, df_oneday, stats_per_season_df)

    #print total runtime
    end_time = time.time()
    print('Runtime:', (end_time - start_time)/60, 'min')

    #export dataframes
    df_stages.to_csv('Test-Data/stages_test.csv', index= True)
    df_oneday.to_csv('Test-Data/oneday_test.csv', index= True)
    stats_per_season_df.to_csv('Test-Data/sps_test.csv', index=True)


def details_sps(dict, df, stats_per_season_df):

    rider_detail_df = pd.DataFrame(columns=["DoB", "weight", "height", "one_day_points", "GC_points", "tt_points",
                                            "sprint_points", "climbing_points", "uci_world_ranking",
                                            "all_time_ranking"])

    # append rider details and get stats per season
    for stage_performance in df.itertuples():
        rider = getattr(stage_performance, "rider_name")

        if rider in dict:
            print(rider, 'in dict')
            details = dict[rider]
            rider_detail_df = pd.concat([rider_detail_df, details], axis=0, ignore_index=True)

        else:
            print(rider)
            details = scrape_rider_details('https://www.procyclingstats.com/rider/{}'.format(rider))
            dict[rider] = details
            rider_detail_df = pd.concat([rider_detail_df, details], axis=0, ignore_index=True)

            stats_per_season = scrape_stats_per_season(
                'https://www.procyclingstats.com/rider/{}/statistics/overview'.format(rider), rider)
            stats_per_season_df = pd.concat([stats_per_season_df, stats_per_season], axis=0, ignore_index=True)

    if df.shape[0] == rider_detail_df.shape[0]:
        df = pd.concat([df, rider_detail_df], axis = 1)


    return dict, df, stats_per_season_df


def prepare_dataframes():

    # prepare dataframe fro stage races
    df_stages = pd.DataFrame(
        columns=['stage_pos', 'gc_pos', 'rider_age', 'team_name', 'rider_name',
                'rider_nationality_code', 'uci_points', 'points', 'striked' 'stage_name',
                'date', 'race_name' 'race_class' 'race_ranking', 'race_country_code',
                'start_location', 'end_location', 'pcs_points_scale', 'profile', 'distance',
                'vertical_meters', 'startlist_quality_score','stage_race','tour', 'tour_code'])

    # prepare dataframe for one day races
    df_oneday = pd.DataFrame(
        columns=['finish_pos', 'rider_age', 'team_name', 'rider_name', 'rider_nationality_code', 'uci_points', 'points',
                 'striked', 'date', 'race_name', 'race_class', 'race_ranking', 'race_country_code', 'start_location',
                 'end_location', 'pcs_points_scale', 'profile', 'distance', 'vertical_meters','startlist_quality_score',
                 'stage_race', 'tour', 'tour_code'])

    # prepare dataframe to hold stats per seasons
    stats_per_season_df = pd.DataFrame(columns=["name", "season", "points", "wins", "racedays"])

    return df_stages, df_oneday, stats_per_season_df


if __name__ == "__main__":
    main()

