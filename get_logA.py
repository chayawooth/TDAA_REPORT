import requests
import datetime
from datetime import datetime, timedelta
from requests.structures import CaseInsensitiveDict
from requests.exceptions import HTTPError

#import urllib3

#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import json


#########################################
#       Function : set date, time to query data start, end
#########################################
def set_query_time():
    start_from = (datetime.today() - timedelta(hours=7, minutes=10)).strftime("%Y-%m-%dT%H:%M:00.000Z")
    to_time = (datetime.now() - timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:00.000Z")
    # Create Dictionary
    req_dict = {
        "version": True,
        "size": 5000,
        "sort": [
            {
                "@timestamp": {
                    "order": "desc",
                    "unmapped_type": "boolean"
                }
            }
        ],
        "aggs": {
            "2": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "20ms",
                    "time_zone": "Asia/Bangkok",
                    "min_doc_count": 1
                }
            }
        },
        "stored_fields": [
            "*"
        ],
        "script_fields": {},
        "docvalue_fields": [
            {
                "field": "@timestamp",
                "format": "date_time"
            },
            {
                "field": "begin_date",
                "format": "date_time"
            }
        ],
        "_source": {
            "excludes": []
        },
        "query": {
            "bool": {
                "must": [],
                "filter": [
                    {
                        "match_all": {}
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_from,
                                "lte": to_time,
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ],
                "should": [],
                "must_not": []
            }
        },
        "highlight": {
            "pre_tags": [
                "@kibana-highlighted-field@"
            ],
            "post_tags": [
                "@/kibana-highlighted-field@"
            ],
            "fields": {
                "*": {}
            },
            "fragment_size": 2147483647
        }
    }
    return json.dumps(req_dict)


#########################################
#       Function : pull_request
#########################################
def pull_request(req_json):
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    #headers["Authorization"] = "Basic bXNvbTpNJDBtQGRtaW4xMzI="
    url = 'https://tuc-tyb2-esdt-vip.tuc.th:9200/elf-tx-tyb-tdaa*/_search'
    try:
        resp = requests.get(url=url, data=req_json, headers=headers, auth=('msom', 'M$0m@dmin132'), verify=False)
        print("this is ",resp)
        resp.raise_for_status()
        respdata = resp.json()
        # print(respdata["msisdn"])
        for key, value in respdata.items():
            print(key, ":", value)

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    print("Print each key-value pair from JSON response")


#########################################
# Main Program
#########################################
req_json = set_query_time()
print(req_json)
print("Main Program \n")
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"
url = 'https://tuc-tyb2-esdt-vip.tuc.th:9200/elf-tx-tyb-tdaa*/_search'
resp = requests.get(url=url, data=req_json, headers=headers, auth=('msom', 'M$0m@dmin132'), verify=False)
print(resp.status_code)
#pull_request(req_json)
