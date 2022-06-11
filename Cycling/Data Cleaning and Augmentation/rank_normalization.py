import os

import numpy as np
import pandas as pd

def rank_normalization():
    '''
    Since all races have a different number of participants that performance of a rider cannot
    be judged just by the finish position (e.g. finishing 20th in a race with 200 participants
    is a significantly better performance than finishing 20th in a race with 20 participants).
    Thus we have to take into account how many other riders participated in a race and normalize
     the finishing position based on this number.
    '''

    for file in os.listdir("Data/onedayraces/"):
            if file.endswith('.csv'):
                try:
                    tmp = pd.read_csv(os.path.join("Data/onedayraces/", file))
                    tmp['finish_pos'] = (tmp['finish_pos']-1)/(tmp.shape[0]-1)

                except: continue

                tmp.to_csv('Data/onedayraces/norm_oneday.csv', mode = 'a')

    for file in os.listdir("Data/Stageraces/"):
            if file.endswith('.csv'):
                try:
                    tmp = pd.read_csv(os.path.join("Data/Stageraces/", file))
                    #normalize stage position
                    tmp['stage_pos'] = (tmp['stage_pos']-1) / (tmp.shape[0]-1)
                    # normalize general classification position
                    tmp['gc_pos'] = (tmp['gc_pos']- 1) / (tmp.shape[0]-1)

                except: continue

                tmp.to_csv('Data/Stageraces/norm_stage.csv', mode = 'a')

    return

def merge_normed_races():
    '''
    Merge One-Day-Races and Stage Races
    '''

    for file in os.listdir("Data/onedayraces/"):
            if file.startswith('norm'):
                norm_oneday= pd.read_csv(os.path.join("Data/onedayraces/", file))
                norm_oneday= norm_oneday[norm_oneday['Unnamed: 0'] != 'Unnamed: 0']
                norm_oneday.drop(columns=['Unnamed: 0','Unnamed: 0.1' ], inplace= True)
    for file in os.listdir("Data/Stageraces/"):
            if file.startswith('norm'):
                norm_stage= pd.read_csv(os.path.join("Data/Stageraces/", file))
                norm_stage.rename(columns= {'stage_pos': 'finish_pos'}, inplace= True)
                norm_stage = norm_stage[norm_stage['Unnamed: 0'] != 'Unnamed: 0']
                norm_stage.drop(columns=['Unnamed: 0','Unnamed: 0.1' ], inplace= True)

    merged_df= pd.merge(norm_stage, norm_oneday, how= 'outer')
    merged_df= merged_df[merged_df['rider_age']!= 'rider_age']
    #merged_df.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'], inplace=True)
    merged_df.to_csv('Data/merged.csv')

    return merged_df

def merge_sps(df):
    '''
    Merge One-Day / Stage Races and Stats per Season by appending
    season stats of a rider to each row
    '''

    for file in os.listdir("Data/sps/"):
        if file.endswith('.csv'):
            try:
                tmp = pd.read_csv(os.path.join("Data/sps/", file))
            except:
                continue
            tmp.to_csv('Data/final_sps.csv', mode='a')

    sps = pd.read_csv('Data/final_sps.csv')

    sps_df = pd.DataFrame()

    for perf in df.itertuples():
        name= getattr(perf, 'rider_name')
        year= str(float(getattr(perf, 'date').split('-')[0]))

        stats = sps[(sps['name']== name) &(sps['season']== year)]
        if stats.shape[0]== 0:

            stats= pd.DataFrame([np.NaN]*3, index = ['season_points', 'season_wins', 'season_racedays']).T
            sps_df = pd.concat([sps_df, stats], axis=0, ignore_index=True)

        else:
            stats.drop(columns=['Unnamed: 0','Unnamed: 0.1', 'name', 'season' ], inplace= True)
            sps_df = pd.concat([sps_df, stats], axis = 0, ignore_index= True)

    sps_df.rename(columns=  {'points':'season_points', 'wins':'season_wins', 'racedays': 'season_racedays'}, inplace= True)
    df =pd.concat([df,sps_df], axis= 1)

    df.to_csv('Data/merged_sps.csv')
    return df



rank_normalization()
merged = merge_normed_races()
merged_sps = merge_sps(merged)


