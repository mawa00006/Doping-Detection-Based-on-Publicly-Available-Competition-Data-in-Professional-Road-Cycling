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

# racename, race year, race difficulty, rider name, age, gender, doping status, ranking, time, captain?, team, uci points, tour code, gc pos, distance, pcs_points, year
def main():
    start_time = time.time()


    #teams_2014 = scrape_teams_for_year(2014)
    #teams_2014.to_csv('Test-Data/teams_2014_test.csv', index = True)

    #riders_team_uae_2022 = scrape_riders_from_team('https://www.procyclingstats.com/team/uae-team-emirates-2022')
    #riders_team_uae_2022.to_csv('riders_team_uae_2022_test.csv', index = True)

    races_2014 = scrape_races_for_year(2014)

    for race in races_2014.itertuples():
        race_data = race
        url = getattr(race, 'race_url')
        stage_race = getattr(race, 'stage_race')
        race_info_df = pd.DataFrame(race).T
        race_info_df = race_info_df.drop(race_info_df.columns[[0,1, 7]], axis = 1)

        if stage_race:
            stage_results, stages  = scrape_stage_race_all_stage_results(url+'/overview', collecting =1)
        else:
            pass
                #TODO
        for i in range(len(stage_results)):
            stage_result = stage_results[i]
            stage = stages.iloc[[i]]
            stage_info_df = pd.DataFrame(stage)
            stage_info_df = pd.concat([stage_info_df]*stage_result.shape[0], ignore_index= True)
            out_df = pd.concat([stage_info_df, stage_result], axis = 1)
            race_inf  = pd.concat([race_info_df]*stage_result.shape[0], ignore_index= True)
            out_df = pd.concat([out_df, race_inf], axis = 1)
            print('test')





    races_2014.to_csv('Test-Data/races_2014_test2.csv', index=True)

    end_time = time.time()


    print('Runtime:', end_time - start_time)

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