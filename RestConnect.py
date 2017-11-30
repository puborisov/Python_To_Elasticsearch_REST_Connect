import json
import csv
import datetime
from elasticsearch2 import Elasticsearch

try:
    es = Elasticsearch(hosts='url', http_auth=('login', 'pass'), port=9200, timeout = 600)
    print('Connected', es.info())
except Exception as ex:
    print('Error:', ex)

now = datetime.datetime.now()

def get_index(index, period, delta):
    if str(period).upper() == 'D':
        return index + '' + '-' + str(now.year) + '.' + str(now.month) + '.' + str(now.day-int(delta))
    else:
        return index + '' + '-' + str(now.year) + '.' + str(now.month-int(delta))


with open('./config/load_config.csv') as csvfile:
    index_file = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in index_file:
        index = get_index(row[0],row[2],row[1])
        print(index)
        
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
        with open(row[3] + str(index) + '.json', 'w') as f:
            json.dump(data, f)