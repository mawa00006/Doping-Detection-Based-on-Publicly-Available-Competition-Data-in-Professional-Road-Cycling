from scraping import get_rider_teams
from scraping import get_rider_years
from scraping import get_rider_details
from scraping import scrape_riders_from_team
from scraping import scrape_one_day_results
from scraping import scrape_race_startlist
from scraping import scrape_stage_race_stage_results
from scraping import scrape_race_information
from scraping import scrape_races_for_year
from scraping import scrape_stage_race_overview_competing_teams
from scraping import scrape_stage_race_overview_stages
from scraping import scrape_stage_race_overview_top_competitors
from scraping import scrape_tour_races_for_year
from scraping import scrape_teams_for_year
from scraping import scrape_rider_all_results

import pandas as pd

# age, gender
def main():


    races_2014 = scrape_races_for_year(2014)
    races_2014.to_csv('Test-Data/races_2014_test.csv', index = True)

    tour_races_for_year_2014 = scrape_tour_races_for_year(2014)
    tour_races_for_year_2014.to_csv('Test-Data/tour_races_for_year_2014.csv', index = True)



if __name__ == "__main__":
    main()
