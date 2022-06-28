import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split


def split_test_train(df, doped = 0):
    '''
    SUMMARY
    Splits dataframe into test and train set
    PARAMETERS
    pd.DataFrame: (df) Dataframe to split
    int: (doped) default = 0, 1 if doped
    OUTPUT
    np.Array: x_train, x_test, y_train, y_test
    '''

    # get all unique rider names
    names = df.rider_name.unique()

    # prepare lists
    x = []
    y = []

    # split data into one sequence per rider per year
    for name in names:

        rider_df = df[df['rider_name'] == name]
        years = rider_df.year.unique()
        rider_df.sort_values(by="date", inplace=True)
        group = rider_df.groupby('year')

        for year in years:
            perf = group.get_group(year)
            if doped:
                y.append(1)
            else:
                y.append(0)
            x.append(perf.to_numpy())

    return train_test_split(x, y, random_state= 1)

###########################################################################################

def load_data():

    # load dataframe
    data = pd.read_csv("Data Cleaning and Augmentation/Data/labeled_data.csv")
    clean = data[data['doped'] == 0]
    doped = data[data['doped'] == 1]

    # get train and test sets
    x_train, x_test, y_train, y_test = split_test_train(clean)
    x_train_doped, x_test_doped, y_train_doped, y_test_doped = split_test_train(doped, 1)

    x_train = np.append(x_train, x_train_doped)
    x_test = np.append(x_test, x_test_doped)
    y_train = np.append(y_train, y_train_doped)
    y_test = np.append(y_test, y_test_doped)

    return x_train, x_test, y_train, y_test


