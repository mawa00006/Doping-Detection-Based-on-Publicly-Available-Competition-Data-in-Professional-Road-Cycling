from bs4 import BeautifulSoup
from requests_html import HTMLSession # to remove
import pandas as pd
import numpy as np
import re
from datetime import datetime

def scrape_races_for_year(year=2020) -> pd.DataFrame:
    """
    SUMMARY
    get details of all races which occurred in a given year (for all available tours).
    E.G. https://www.procyclingstats.com/races.php?year=2020
    PARAMETERS
    year (int): year to get races for (default=2020)
    OUTPUT
    pandas.DataFrame: fetched data includes:
                        "stage_race" (bool) whether race is a stage race of not
                        "race_url" (str) full url to race overview page
                        "tour" (str) name of tour race occured in
                        "tour_code" (int) PCS code for tour
    """
    years=get_available_tours_for_year(year)

    # prepare data frame
    df=pd.DataFrame()
    i = 0
    for key,value in years.items():
        # drop all junior races
        if value in [15,16,17]:
            continue

        print("{}             ".format(key),end="\r")
        year_race_series=scrape_tour_races_for_year(year=year,tour_code=value)
        year_race_series["tour"]=key
        year_race_series["tour_code"]=value
        df=pd.concat([df,year_race_series],ignore_index=True)

    #drop all cancelled races
    df = df[df["cancelled"]== False]
    df.drop("cancelled", axis = 1, inplace= True)

    return df

def get_available_tours_for_year(year=2020) -> {str:int}:
    """
    SUMMARY
    get details for all tours which occured in a given year.
    E.G. https://www.procyclingstats.com/races.php?year=2020
    PARAMETERS
    year (int): year to get tours for (default=2020)
    OUTPUT
    {str:int}: dictionary from `tour_name` to `tour_code`
    """
    # format url
    url="https://www.procyclingstats.com/races.php?year={}".format(year)

    # fetch data
    session=HTMLSession()
    response=session.get(url)
    response.html.render()
    soup=BeautifulSoup(response.html.html,"lxml")

    # isolate input field
    select_field=soup.find("select",{"name":"circuit"})
    select_field_options=select_field.find_all("option")

    # prepare dict
    tours={}

    # fill dict
    for option in select_field_options:

        if len(option["value"]) == 0:
            continue
        tours[option.text]=int(option["value"])

    return tours

def scrape_tour_races_for_year(year=2020,tour_code=1) -> pd.DataFrame:
    """
    SUMMARY
    get details for all races which occured in a given tour, in a given year
    E.G. https://www.procyclingstats.com/races.php?year=2020&circuit=1
    PARAMETERS
    year (int): year to get races from (default=2020)
    tour_code (int): PCS code for tour to get details of (default=1)
    OUTPUT
    pandas.DataFrame: fetched data includes:
                        "stage_race" (bool) whether race is a stage race of not
                        "race_class" (str) classification of race
                        "cancelled" (bool) whether race was/is cancelled
                        "race_url" (str) full url to race overview page
    """
    # format url
    url="https://www.procyclingstats.com/races.php?year={}&circuit={}".format(year,tour_code)

    # fetch data
    session=HTMLSession()
    response=session.get(url)
    response.html.render()
    soup=BeautifulSoup(response.html.html,"lxml")

    table_div=soup.find("table",{"class":"basic"})
    table_body=table_div.find("tbody")
    table_rows=table_body.find_all("tr")

    df=pd.DataFrame(columns=["stage_race","cancelled","race_url"])

    for row in table_rows:
        series=pd.DataFrame(parse_tour_races_for_year_row(row)).T
        df=pd.concat([df,series],axis=0,ignore_index=True)

    return df

def parse_tour_races_for_year_row(row) -> pd.Series:
    """
    SUMMARY
    parse details from row of table of races in a given year & tour
    used by Scraper.scrape_tour_races_for_year
    PARAMETERS
    row (bs4.element.Tag): row from table
    OUTPUT
    pandas.Series: fetched data includes:
                        "stage_race" (bool) whether race is a stage race of not
                        "cancelled" (bool) whether race was/is cancelled
                        "race_url" (str) full url to race overview page
    """
    series=pd.Series(dtype= 'object')

    row_details=row.find_all("td")

    # extract details
    try:
        series["cancelled"]= ("striked" in row["class"])
    except:
        series["cancelled"]= False
    series["stage_race"]=("-" in row_details[0].text)
    series["race_url"]="https://www.procyclingstats.com/"+row_details[2].find("a")["href"]

    return pd.Series(series)


def scrape_race_information(url:str):
    """
    SUMMARY
    scrape race information from a one day race url
    E.G. https://www.procyclingstats.com/race/milano-sanremo/2014
    PARAMETERS:
    url (str): url for a one day race
    OUTPUT
    pandas.Dataframe: fetched data includes
                    "date" () date of the race
                    "race_name" (str) name of race
                    "race_class" (str) classification of race
                    "race_ranking" (int) ranking of race
                    "race_country_code" (str) code for host country
                    "start_location" (str) name of start town
                    "end_location" (str) name of finish town
                    "pcs_points_scale" (str) name of points scale being used
                    "profile" (str) code for profile of race
                    "distance" (int) distance of stage in km
                    "vertical_meters" (int) vertically climbed distance of stage in km
                    "startlist_quality-score" (int) quality of startlist

    """
    try:
        # start session
        session = HTMLSession()
        response = session.get(url)
        response.html.render()
        soup = BeautifulSoup(response.html.html, "lxml")

        #series to fill in
        series = pd.Series(dtype= 'object')

        #isolate main info
        main = soup.find("div",{"class":"main"})

        # isolate race information table
        infolist = soup.find("ul", {"class": "infolist"}).find_all("div")
    except Exception as e:
        print('scrape_race_information: Exception occured while trying to open race page:', e)
        return pd.Series([np.NaN]*12)

    #scrape data
    try: series["date"] = datetime.strptime(infolist[1].text, '%d %B %Y').date()
    except: series['date'] = np.Nan
    try: series["race_name"] = main.find("h1").text
    except: series["race_name"] = np.NaN
    try: series["race_class"] = main.find_all("font")[1].text
    except: series["race_class"] = np.NaN
    try: series["race_ranking"] = infolist[23].text
    except: series["race_ranking"] = np.NaN
    try: series["race_country_code"] = main.find("span",{"class":"flag"})["class"][1]
    except: series["race_country_code"] = np.NaN
    try: series["start_location"] = infolist[19].text
    except: series["start_location"] = np.NaN
    try: series["end_location"] = infolist[21].text
    except: series["end_location"] =  np.NaN
    try: series["pcs_points_scale"] = infolist[11].text
    except: series["pcs_points_scale"] = np.NaN
    try: series["profile"] = infolist[13].contents[0].attrs['class'][2]
    except: series["profile"] = np.NaN
    try: series["distance"] = infolist[9].text
    except: series["distance"] = np.NaN
    try: series["vertical_meters"] = infolist[17].text
    except: series["vertical_meters"] = np.NaN
    try: series["startlist_quality_score"] = infolist[25].text
    except: series["startlist_quality_score"] = np.NaN

    return pd.Series(series)

def scrape_stage_race_names_urls(url:str) -> pd.DataFrame:
    """
    SUMMARY
    get details for stages in a stage race from it's overview page
    E.G. https://www.procyclingstats.com/race/tour-de-france/2019/overview
    PARAMETERS
    url (str): url for a race's overview page
    OUTPUT
    type: description
    pandas.DataFrame: fetched data includes
                        "stage_name" (str) name of stage (`stage #` or `REST DAY`)
                        "stage_url" (str) full url to stage's detail page
    """
    try:
        # fetch data
        session=HTMLSession()
        response=session.get(url)
        response.html.render()
        soup=BeautifulSoup(response.html.html,"lxml")

        # isolate desired list
        left_div=soup.find_all("div",{"class":"mt20"})[1]
        stage_list=left_div.find_all("ul")

        # get list items
        stage_list_items=left_div.find_all("li")
    except Exception as e:
        print('scrape_stage_race_names_urls: Exception occured while trying to open race page:', e)
        return pd.DataFrame([np.NaN] * 2).T

    # prepare data frame
    df=pd.DataFrame(columns=["stage_name","stage_url", "TTT"])

    # fill data frame

    for list_item in stage_list_items:
        pattern = r'[0-9]'
        test = re.sub(pattern, '',list_item.text)
        try:
            if  test!= '/Restday': series=parse_stage_list_item(list_item) # not a rest day
            else: series=pd.Series({"stage_name":"REST DAY"}) # is a rest day
            series= pd.DataFrame(series).T
            df= pd.concat([df,series],ignore_index=True)
        except:
            return pd.DataFrame([np.NaN] * 2).T

    df =df[(df["stage_name"] != "REST DAY") & (df["TTT"] != True)]
    df.drop("TTT", axis=1, inplace=True)
    return df

def parse_stage_list_item(list_item) -> pd.Series:
    """
    SUMMARY
    get details about a single stage
    USED by Scraper.scrape_stage_race_overview_stages
    PARAMETERS
    list_item (bs4.element.Tag): stage item from list of stages
    OUTPUT
    type: description
    pandas.Series: fetched data includes
                        "stage_url" (str) full url to stage's detail page
                        "stage_name" (str) name of stage (`stage #` or `REST DAY`)
                        "TTT" (boolean) Team Time Trial or not
    """
    series=pd.Series(dtype= 'object')


    # url
    stage_details=list_item.find("a")
    series["stage_url"]="https://www.procyclingstats.com/"+stage_details["href"]

    # stage name
    stage_detail_divs=list_item.find_all("div")
    locations=stage_detail_divs[2].text.split("|")
    series["stage_name"]=locations[0]

    # Team time trial
    if re.search("(TTT)",locations[0]):
        series["TTT"]= True
    else:
        series["TTT"]= False

    return pd.Series(series)



def scrape_stage_race_all_stage_results(url:str) -> [pd.DataFrame]:
    """
    SUMMARY
    get finishing results for each stage in a stage race.
    E.G. https://www.procyclingstats.com/race/tour-de-france/2020/overview
    PARAMETERS
    url (str): full url to stage race overview
    OUTPUT
    type: description
    list(pandas.DataFrame): one dataframe for results for each stage. each dataframe includes
    """
    stages=scrape_stage_race_names_urls(url)

    results=[]


    # scrape data for 'normal stages'
    for stage in stages.itertuples():
        stage_url = stage[2]
        if stage_url[:4]!="http": stage_url="https://"+stage_url
        print(stage_url)
        try:stage_results_df=scrape_stage_race_stage_results(stage_url)
        except:continue
        try:info_df = pd.DataFrame(scrape_race_information(stage_url)).T
        except: continue
        info_df.insert(0, "stage_name", stage[1])
        info_df = pd.concat([info_df]*stage_results_df.shape[0], ignore_index= True)
        stage_results_df = pd.concat([stage_results_df, info_df], axis= 1)

        results.append(stage_results_df)

    return results

def scrape_stage_race_stage_results(url:str) -> pd.DataFrame:
    """
    SUMMARY
    get finish results for individual stage of a stage race
    E.G. https://www.procyclingstats.com/race/tour-de-france/2020/stage-5
    PARAMETERS
    url (str): full url for a stage
    OUTPUT
    type: description
    pandas.DataFrame: fetched data includes
                        "stage_pos" (int) finish position of rider (`np.NaN` if rider didn't finish stage)
                        "gc_pos" (int) rider's gc position after stage (`np.NaN` if rider didn't finish stage)
                        "rider_age" (int) rider's age on day of stage
                        "team_name" (str) name of rider's team
                        "rider_name" (str) name of rider
                        "rider_nationality_code" (str) PCS code for rider's nationality
                        "uci_points" (int) number of uci points won by rider in stage
                        "points" (int) number of PCS points won by rider in stage
                        "striked" (int) result was stripped (doping?)
    """
    try:
        # start session
        session=HTMLSession()
        response=session.get(url)
        response.html.render()
        soup=BeautifulSoup(response.html.html,"lxml")
        infos = soup.find('w30 right mg_rp10')
        # isolate desired table
        table=soup.find("table")
        if (table is None):
            print('scrape_stage_race_stage_results:Table is None', url)
            return None # results don't exist

        results_table=table.find("tbody")
        rows=results_table.find_all("tr")
    except Exception as e:
        print('scrape_stage_race_names_urls: Exception occured while trying to open race page', e)
        return None

    # prepare data frame
    df=pd.DataFrame(columns=["stage_pos","gc_pos","rider_age","team_name","rider_name",
                             "rider_nationality_code","uci_points","points", "striked"])

    # fill data frame
    for row in rows:
        try:
            series=pd.DataFrame(parse_stage_race_stage_results_row(row)).T
        except Exception as e:
            print('Exception occured while parsing stageraceresultsrow', e)
            continue
        df=pd.concat([df,series],ignore_index=True)

    return df

def parse_stage_race_stage_results_row(row) -> pd.Series:
    """
    SUMMARY
    parse data from row of stage results table
    USED by Scraper.scrape_stage_race_stage_results
    PARAMETERS
    row (bs4.element.Tag): row to extract details from
    OUTPUT
    pandas.Series: fetched data includes
                        "stage_pos" (int) finish position of rider
                        "gc_pos" (int) rider's gc position after stage
                        "rider_age" (int) rider's age on day of stage
                        "team_name" (str) name of rider's team
                        "rider_name" (str) name of rider
                        "rider_nationality_code" (str) PCS code for rider's nationality
                        "uci_points" (int) number of uci points won by rider in stage
                        "points" (int) number of PCS points won by rider in stage
                        "striked" (int) result was stripped (doping?)
    """
    series=pd.Series(dtype= 'object')
    row_data=row.find_all("td")
    striked = row.find_all("s")

    # race details
    stage_pos=row_data[0].text
    gc_pos=row_data[1].text

    try:
        series["stage_pos"]=int(stage_pos) if (stage_pos not in ["DNF","OTL","DNS","DF", "DSQ"]) else np.NaN
    except:
        series["stage_pos"]= np.NaN

    series["gc_pos"]=int(gc_pos) if (gc_pos!="") else np.NaN

    # rider and team details
    try: series["rider_age"]=row_data[6].text
    except: series["rider_age"]= np.NaN
    try: series["team_name"]=row_data[7].text
    except: series["team_name"]= np.NaN
    try: series["rider_name"]=row_data[5].find('a').attrs['href'].split('/')[1]
    except: series["rider_name"] = np.NaN
    try: series["rider_nationality_code"]=row_data[5].find("span",{"class":"flag"})["class"][-1]
    except: series["rider_nationality_code"]= np.NaN

    # point results
    uci_points=row_data[8].text
    points=row_data[9].text
    series["uci_points"]=int(uci_points) if (uci_points!="") else 0
    series["points"]=int(points) if (points!="") else 0
    if len(striked) != 0:
        series["striked"]= 1
    else:
        series["striked"]= 0

    return pd.Series(series)

"""
ONE DAY RACING
"""

def scrape_one_day_results(url:str) -> pd.DataFrame:
    """
    SUMMARY
    get finish results for a one day race, from its results page
    E.G. https://www.procyclingstats.com/race/gp-samyn/2020/result
    PARAMETERS
    url (str): full url for a one day race results page
    OUTPUT
    type: description
    pandas.DataFrame: fetched data includes
                        "finish_pos" (int) finish position of rider (`np.NaN` if rider didn't finish stage)
                        "rider_age" (int) rider's age on day of stage
                        "team_name" (str) name of rider's team
                        "rider_name" (str) name of rider
                        "rider_nationality_code" (str) PCS code for rider's nationality
                        "uci_points" (int) number of uci points won by rider in stage
                        "points" (int) number of PCS points won by rider in stage
                        "striked" (boolean) true if performance was striked out
                        "date" () date of the race
                        "race_name" (str) name of race
                        "race_class" (str) classification of race
                        "race_ranking" (int) ranking of race
                        "race_country_code" (str) code for host country
                        "start_location" (str) name of start town
                        "end_location" (str) name of finish town
                        "pcs_points_scale" (str) name of points scale being used
                        "profile" (str) code for profile of race
                        "distance" (int) distance of stage in km
                        "vertical_meters" (int) vertically climbed distance of stage in km
                        "startlist_quality-score" (int) quality of startlist
    """
    try:
        # start session
        session=HTMLSession()
        response=session.get(url)
        response.html.render()
        soup=BeautifulSoup(response.html.html,"lxml")
        print(url)

        # isolate desired table
        table=soup.find("table")
        if (table is None): return None # results don't exist

        results_table=table.find("tbody")
        rows=results_table.find_all("tr")
    except:
        print('scrape_one_day_resuts: table is None', url)
        return None
    # prepare data frame
    df=pd.DataFrame(columns=["finish_pos","rider_age","team_name","rider_name","rider_nationality_code","uci_points","points", "striked"])

    # get race information
    try:
        race_inf = scrape_race_information(url)
    except:
        return None
    # fill data frame
    for row in rows:
        try:
            series=parse_one_day_results_row(row)
        except: continue
        series = pd.DataFrame(pd.concat([race_inf, series], axis=0)).T
        df=pd.concat([df,series],axis =0,ignore_index=True)


    return df

def parse_one_day_results_row(row) -> pd.Series:
    """
    SUMMARY
    parse data from row of one-day results table
    USED by Scraper.scrape_stage_race_stage_results
    PARAMETERS
    row (bs4.element.Tag): row to extract details from
    OUTPUT
    pandas.Series: fetched data includes
                        "finish_pos" (int) finish position of rider (`np.NaN` if rider didn't finish stage)
                        "rider_age" (int) rider's age on day of stage
                        "team_name" (str) name of rider's team
                        "rider_name" (str) name of rider
                        "rider_nationality_code" (str) PCS code for rider's nationality
                        "uci_points" (int) number of uci points won by rider in stage
                        "points" (int) number of PCS points won by rider in stage
                        "striked" (boolean) true if performance was striked out
    """
    series=pd.Series(dtype= 'object')
    row_data=row.find_all("td")
    striked = row.find_all("s")
    # race details
    finish_pos=row_data[0].text
    series["finish_pos"]=int(finish_pos) if (finish_pos not in ["DF","DNF","OTL","DNS", "DSQ"]) else np.NaN

    # rider and team details
    series["team_name"]=row_data[5].text
    series["rider_name"]=row_data[3].find('a').attrs['href'].split('/')[1]
    series["rider_nationality_code"]=row_data[3].find("span",{"class":"flag"})["class"][-1]
    series["rider_age"]=int(row_data[4].text) if (len(row_data[4].text) == 2) else np.NaN

    # point results
    uci_points=row_data[6].text
    points=row_data[7].text
    series["uci_points"]=int(uci_points) if (uci_points!="") else 0
    series["points"]=int(points) if (points!="") else 0

    #striked performance
    if len(striked) != 0:
        series["striked"]= 1
    else:
        series["striked"]= 0

    return pd.Series(series)

def scrape_rider_details(url:str):
    """
    SUMMARY
    Scrapes rider detail from riders page
    PARAMETERS
    url to riders overview page E.G. https://www.procyclingstats.com/rider/tadej-pogacar
    OUTPUT
    pandas.Dataframe: fetched data includes
                    "DoB" Date of Birthday
                    "weight" (int) riders weight in kg
                    "height" (int) riders height in cm
                    "one_day_points" (int) total UCI points from one day races
                    "GC_points"  (int) total UCI points from general classifications
                    "tt_points"  (int) total UCI points from time trials
                    "sprint_points" (int) total UCI points from sprints
                    "climbing_points" (int) total UCI points from climbing races
                    "uci_world_ranking" (int)  current UCI world ranking
                    "all_time_ranking" (int) all time UCI ranking
    """
    try:
        # start session
        session = HTMLSession()
        response = session.get(url)
        response.html.render()
        soup = BeautifulSoup(response.html.html, "lxml")

        series = pd.Series(dtype= 'object')

        div = soup.find("div",{"class":"rdr-info-cont"})
        span = div.find_all("span")
        pps = div.find_all("div", {"class":"pnt"})
        rnk = div.find_all("div",{"class": "rnk"} )
    except:pd.DataFrame([np.NaN]*10).T    # date of birth
    try:
        d = div.contents[1]
        m = div.contents[3].split(" ")[1]
        y = div.contents[3].split(" ")[2]
        series["DoB"]= "{}.{}.{}".format(d,m,y)
    except:
        series["DoB"]= np.NaN

    #weight and height
    try:
        series["weight"] = span[1].contents[1].split(" ")[1]
        series["height"] = span[2].contents[1].split(" ")[1]
    except:
        series["weight"] = np.NaN
        series["height"] = np.NaN

    # points per speciality
    series["one_day_points"] = int(pps[0].text)
    series["GC_points"] = int(pps[1].text)
    series["tt_points"] = int(pps[2].text)
    series["sprint_points"] = int(pps[3].text)
    series["climbing_points"] = int(pps[4].text)

    #world ranking
    try: # not all riders are ranked, some have an additional PCS ranking
        if len(rnk)== 3:
            series["uci_world_ranking"] = int(rnk[1].text)
            series["all_time_ranking"] = int(rnk[2].text)
        else:
            series["uci_world_ranking"] = int(rnk[0].text)
            series["all_time_ranking"] = int(rnk[1].text)
    except:
        series["uci_world_ranking"] = np.NaN
        series["all_time_ranking"] = np.NaN


    return pd.DataFrame(series).T


def scrape_stats_per_season(url:str, rider_name):
    """
    SUMMARY
    scrape stats per season table from rider overview page
    PARAMTERES
    urls to rider overview page E.G. https://www.procyclingstats.com/rider/tadej-pogacar/statistics/overview
    OUTPUT
    pd.Dataframe: fetched data includes
                "season" (int) year
                "points" (int) sum of points in a given season
                "wins" (int) wins in a given season
                "racedays" (int) racedays in a given season
    """
    try:
        # start session
        session = HTMLSession()
        response = session.get(url)
        response.html.render()
        soup = BeautifulSoup(response.html.html, "lxml")

        # prepare df
        sps_df = pd.DataFrame(columns=["season", "points", "wins", "racedays"])

        # get table
        table = soup.find("div", {"class":"mt10"}).find("tbody")
        rows = table.find_all("tr")
    except: return pd.DataFrame([np.NaN]*4).T
    for row in rows:
        try:
            df = pd.DataFrame(parse_stats_per_season_row(row)).T
        except: continue
        df.insert(0, "name", rider_name)
        sps_df = pd.concat([sps_df,df ], axis= 0, ignore_index= True)

    return sps_df

def parse_stats_per_season_row(row):
    """
    SUMMARY
    parse a row in Stats per season table from rider overview page
    PARAMETERS
    row from the the table
    OUTPUT
    pd.Series: parsed data includes
                "season" (int) year
                "points" (int) sum of points in a given season
                "wins" (int) wins in a given season
                "racedays" (int) racedays in a given season

    """
    series = pd.Series(dtype= 'object')

    row= row.find_all("td")

    try:series["season"] = row[0].text
    except: series["season"] = np.NaN
    try:series["points"] = row[1].text
    except: series["points"] = np.NaN
    try:series["wins"] = row[4].text
    except: series["wins"] = np.NaN
    try:series["racedays"] = row[5].text
    except: series["racedays"] = np.NaN

    return series

