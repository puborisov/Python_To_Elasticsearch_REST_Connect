import json
import pandas as pd
import csv
import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

# Set up some logging
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('./logs/ParseJSON-{:%Y.%m.%d}.log'.format(datetime.now()))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

with open('./config/parse_config.csv') as csvfile:
    parse_config = csv.reader(csvfile, delimiter=',')
    for config in parse_config:

        for filename in os.listdir(config[1]):
            file_name = str(filename)
            
            logger.info(config[1]+file_name)
            
            
            # Get file date
            if (re.search(r'\d{4}.\d{2}.\d{2}', file_name) is None):
                match = re.search(r'\d{4}.\d{2}', file_name)
            else:
                match = re.search(r'\d{4}.\d{2}.\d{2}', file_name)
                
            file_date = str(match.group())
            
            # Load data from json file              
            with open(config[1]+file_name, encoding='utf8') as file:
                data_load = json.load(file)

            # Parse JSON scroll records
            data = []
            for i in range(len(data_load)):
                data += data_load[i]['hits']['hits']

            # Init dic with dataframes
            df = {}

            # Create main dataframe
            df['main'] = pd.io.json.json_normalize(data)

            # Iterate over all DataFrames
            d = 0
            while d < len(df.keys()):

                # Move list to "out" dict and mark their positions
                out = {}
                cur_df = list(df.keys())[d]

                for col in df[cur_df].columns:
                    if col != '__value__'  and any(isinstance(n, list) for n in df[cur_df][col]):
                        out[col] = df[cur_df][col]
                        df[cur_df].drop(col, axis=1, inplace=True)


                # Parse "out" data

                for key in out.keys():
                    df_to_add = []
                    dict_flag = True
                    for list_num in range(len(out[key])):
                        if isinstance(out[key][list_num], list) and (len(out[key][list_num])>0):
                            if any(isinstance(n, dict) for n in out[key][list_num]):
                                for list_item in out[key][list_num]:
                                    list_item.update({"_id": str(df['main'].iloc[int(list_num)]['_id'])})
                                df_to_add += out[key][list_num]
                            else:
                                dict_flag = False
                                df_to_add += list(map(list,
                                                      zip(*[out[key][list_num],[df['main'].iloc[int(list_num)]['_id']]
                                                            * len(out[key][list_num])])))

                    if dict_flag:
                        df['{}.{}'.format(cur_df, key)] = pd.DataFrame(df_to_add)
                    else:
                        df['{}.{}'.format(cur_df, key)] = pd.DataFrame(df_to_add, columns=['__value__','_id'])
                d += 1

            # Save to files
            for key, value in df.items():
                file_path = '{}{}_{}.csv'.format(config[2], key, file_date)
                value.to_csv(file_path, encoding='utf-8', index=False)
            
            # Delete source file
            os.remove(config[1]+file_name)
            logger.info('=> JSON parsed')