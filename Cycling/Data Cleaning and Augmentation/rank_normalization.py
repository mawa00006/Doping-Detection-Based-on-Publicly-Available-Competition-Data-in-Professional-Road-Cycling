import os
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

    for file in os.listdir("Data/onedayraces/"):
            if file.startswith('norm'):
                norm_oneday= pd.read_csv(os.path.join("Data/onedayraces/", file))
                norm_oneday= norm_oneday[norm_oneday['Unnamed: 0'] != 'Unnamed: 0']
                norm_oneday.drop(columns=['Unnamed: 0','Unnamed: 0.1' ], inplace= True)
    for file in os.listdir("Data/Stageraces/"):
            if file.startswith('norm'):
                norm_stage= pd.read_csv(os.path.join("Data/Stageraces/", file))
                norm_stage.rename(columns= {'stage_pos': 'finish_pos'})
                norm_stage = norm_stage[norm_stage['Unnamed: 0'] != 'Unnamed: 0']
                norm_stage.drop(columns=['Unnamed: 0','Unnamed: 0.1' ], inplace= True)

    merged_df= pd.merge(norm_stage, norm_oneday, how= 'outer')
    merged_df= merged_df[merged_df['rider_age']!= 'rider_age']
    #merged_df.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'], inplace=True)
    merged_df.to_csv('Data/merged.csv')

    return

rank_normalization()
merge_normed_races()
