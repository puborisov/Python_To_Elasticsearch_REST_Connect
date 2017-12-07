import json
import csv
from datetime import datetime
from elasticsearch2 import Elasticsearch
from dateutil.relativedelta import relativedelta
import logging

# Set up some logging
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('./logs/RestConnect-{:%Y.%m.%d}.log'.format(datetime.now()))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

# Set connection
try:
    es = Elasticsearch(hosts='host', http_auth=('login', 'pass'), port=9200, timeout = 600)
    logger.info('Connected: ', es.info())
except Exception as ex:
    logger.error(ex)


def get_index(index, period, delta):
    if str(period).upper() == 'D':
        date = datetime.now() + relativedelta(days=-int(delta))
        return '{}-{:%Y.%m.%d}'.format(index,date)
    else:
        date = datetime.now() + relativedelta(months=-int(delta))
        return '{}-{:%Y.%m}'.format(index, date)

# Cycle through all indexes
with open('./config/load_config.csv') as csvfile:
    
    index_file = csv.reader(csvfile, delimiter=',', quotechar='|')
    indexes = []

    for index_params in index_file:
        indexes.append([index_params[0],index_params[1],index_params[2],index_params[3]])
        if (str(index_params[2]).upper() != 'D') and (datetime.now().day==1):
            indexes.append([index_params[0],'1','m',index_params[3]])
            
    for index_params in indexes:
        
        try:
            index = get_index(index_params[0],index_params[2],index_params[1])
            logger.info(index)

            # Initialize the request
            query_body = '{"query": {"match_all": {}}}'
            scroll = '1m'
            timeout = 6000
            size = 100
            
            page = es.search(index=index, scroll=scroll, size=size, body=query_body, request_timeout=timeout)

            # Init scroll
            sid = page['_scroll_id']
            scroll_size = page['hits']['total']

            # Init data
            data = []
            data.append(page)

            # Start scrolling
            while (scroll_size > 0):
                page = es.scroll(scroll_id=sid, scroll=scroll)

                # Update the scroll ID
                sid = page['_scroll_id']

                # Get the number of results that we returned in the last scroll
                scroll_size = len(page['hits']['hits'])

                # Add to data list
                data.append(page)

            # Save to file
            with open(index_params[3] + str(index) + '.json', 'w') as f:
                json.dump(data, f)
            logger.info('=> Loaded successfully')
        except Exception as ex:
            logger.error(ex)