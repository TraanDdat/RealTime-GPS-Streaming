import numpy as np
import pandas as pd
import os

if __name__ == '__main__':
    columns = ['User_ID', 'Date', 'Latitude', 'Longitude', 'Altitude', 'Timestamp']
    df = pd.read_csv('.\\merged_points.csv',usecols=columns)
    print(df)
    for i in range(0,182):
        user_id = i
        selected_user = df[df['User_ID'] == user_id].copy()
        sorted_selected_user = selected_user.sort_values(by=['Timestamp'],ascending=True)
        os.makedirs('.\\data_csv', exist_ok=True)
        sorted_selected_user.to_csv(f'.\\data_csv\\User_{i}.csv',index=False)