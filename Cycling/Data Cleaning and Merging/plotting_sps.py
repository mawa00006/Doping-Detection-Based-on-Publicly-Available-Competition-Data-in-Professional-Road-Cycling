import pandas as pd
import numpy as np
import matplotlib.pylab as plt

def prepare_dataframes():

  sps = pd.read_csv('Data/labeled_sps.csv')
  sps.drop_duplicates(inplace=True)

  #get set of riders
  rider_names = sps.name.unique()

  #prepare dataframes

  wins = pd.DataFrame(columns = np.arange(0, 30, 1))
  points = pd.DataFrame(columns = np.arange(0, 30, 1))
  racedays = pd.DataFrame(columns = np.arange(0, 30, 1))

  wins_doped = pd.DataFrame(columns = np.arange(0, 30, 1))
  points_doped = pd.DataFrame(columns = np.arange(0, 30, 1))
  racedays_doped = pd.DataFrame(columns = np.arange(0, 30, 1))

  for rider in rider_names:
    rider_sps = sps[sps['name'] == rider]
    rider_sps.sort_values(by= 'season',inplace= True , ignore_index= True)
    rider_sps.drop(rider_sps.tail(1).index, inplace=True)

    rider_wins =pd.DataFrame(rider_sps.wins).rename(columns={'wins': rider}).T
    rider_points = pd.DataFrame(rider_sps.points).rename(columns={'points': rider}).T
    rider_racedays = pd.DataFrame(rider_sps.racedays).rename(columns={'racedays': rider}).T

    if np.sum(rider_sps.doped) == 0:
      wins = pd.concat([wins, rider_wins])
      points = pd.concat([points, rider_points])
      racedays = pd.concat([racedays, rider_racedays])

    else:
      wins_doped = pd.concat([wins_doped, rider_wins])
      points_doped = pd.concat([points_doped, rider_points])
      racedays_doped = pd.concat([racedays_doped, rider_racedays])

  wins.to_csv('Data/wins.csv', index= False)
  points.to_csv('Data/points.csv', index= False)
  racedays.to_csv('Data/racedays.csv', index= False)
  wins_doped.to_csv('Data/wins_doped.csv', index= False)
  racedays_doped.to_csv('Data/racedays_doped.csv', index= False)
  points_doped.to_csv('Data/points_doped.csv', index= False)

def plot(name):
  doped = pd.read_csv('Data/{}_doped.csv'.format(name))
  not_doped = pd.read_csv('Data/{}.csv'.format(name))
  test = doped.describe()

  description_doped =doped.describe().loc[['mean']]
  description = not_doped.describe().loc[['mean']]
  stde_doped = doped.describe().loc[['std']]
  stde= not_doped.describe().loc[['std']]
  y = []
  z = []


  for i in np.arange(0, 30, 1):
    y.append( getattr(description_doped, '{}'.format(i)))
    z.append(getattr(description, '{}'.format(i)))


  x = np.arange(0, 30, 1)

  # Create Plot

  fig, ax = plt.subplots()

  ax.set_xlabel('Career Length')
  ax.set_ylabel('{}'.format(name), color='green')
  plot_1 = ax.plot(x, y, color='red', label = 'doped')
  plot_1 = ax.plot(x, z, color='green', label= 'clean')
  ax.tick_params(axis='y', labelcolor='green')
  ax.legend()

  # Show plot

  plt.show()
prepare_dataframes()
plot('wins')
plot('points')
plot('racedays')