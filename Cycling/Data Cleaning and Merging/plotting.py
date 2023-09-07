import pandas as pd
from matplotlib import pyplot as plt
import numpy as np


df = pd.read_csv('Data/labeled_data.csv')



number_datapoints = df.shape[0]
number_doped = df[df['doped']== 1].shape[0]

plt.bar(['Total', 'ADRV'],[number_datapoints, number_doped], color =['dimgrey', 'darkgrey'])

plt.show()
print(number_doped/number_datapoints)


class_one =df[df['race_class'].isin(['(2.UWT)' ,'(WT)' , '(WC)' ,'(1.UWT)' , '(1.Pro)' , '(2.Pro)' ])]

class_one_clean = class_one[class_one['doped']== 0].rider_name.unique().shape[0]
class_one_doped = class_one[class_one['doped']== 1].rider_name.unique().shape[0]

plt.bar(['Clean', 'Doped'], [class_one_clean, class_one_doped], color =['dimgrey', 'darkgrey'])
plt.show()
print(class_one_doped/(class_one_clean+class_one_doped))
print('test')