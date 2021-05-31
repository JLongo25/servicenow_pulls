#!/bin/python3
import requests
import json
from dateutil import parser
from elasticsearch import helpers
from datetime import date, timedelta
from es_connect import connect_elasticsearch
from helper import log, u_log


def get_table(table):
    url = f"https://onepoint.service-now.com/api/now/table/{table}?sysparm_query=sys_domain%3D7e627c2a1bc2c090fba6337bcd4bcb61^sys_created_onBETWEENjavascript%3Ags.dateGenerate('{first.strftime('%Y-%m-%d')}'%2C'00%3A00%3A00')%40javascript%3Ags.dateGenerate('{last.strftime('%Y-%m-%d')}'%2C'23%3A59%3A59')&sysparm_display_value=true&sysparm_exclude_reference_link=true&sysparm_fields=number%2Csys_domain%2Cstate%2Csys_created_on%2Copened_by%2Cpriority%2Cshort_description' \
          '%2Cassignment_group%2Csys_class_name%2Ccompany%2Cactive%2Csys_updated_on"
    url_opened = f"https://onepoint.service-now.com/api/now/table/{table}?sysparm_query=sys_domain%3D7e627c2a1bc2c090fba6337bcd4bcb61^active=true^sys_class_name!=sc_req_item^sys_class_name!=sc_request^state!=6&sysparm_display_value=true&sysparm_exclude_reference_link=true&sysparm_fields=number%2Csys_domain%2Cstate%2Csys_created_on%2Copened_by%2Cpriority%2Cshort_description' \
          '%2Cassignment_group%2Csys_class_name%2Ccompany%2Cactive%2Csys_updated_on"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    response = requests.get(url, auth=(u_log(), log()), headers=headers)
    response_update = requests.get(url_opened, auth=(u_log(), log()), headers=headers)
    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()
    create = json.loads(response.content.decode('utf-8'))
    opened = json.loads(response_update.content.decode('utf-8'))
    return create, opened


def empty_index(index):
    body = {
        "query": {
            "match_all": {}
        }
    }
    es.delete_by_query(index=index, body=body)


# get last months date range
first_day = date.today().replace(day=1)
last = first_day - timedelta(days=1)
first = last.replace(day=1)

queries = get_table('task')

created = queries[0]['result']
opens = queries[1]['result']

for i in range(len(created if len(created) > len(opens) else opens)):
    for t in queries:
        try:
            epoch = parser.parse(t['result'][i]['sys_created_on'])
            epoch1 = parser.parse(t['result'][i]['sys_updated_on'])
            t['result'][i]['sys_created_on'] = epoch.strftime('%s')
            t['result'][i]['sys_updated_on'] = epoch1.strftime('%s')
            t['result'][i]['timestamp'] = t['result'][i]['sys_created_on']
        except IndexError:
            pass


es = connect_elasticsearch()

resp = helpers.bulk(es, created, index='index', doc_type='_doc')

empty_index('smile_snow_open_task')
resp1 = helpers.bulk(es, opens, index='index', doc_type='_doc')
