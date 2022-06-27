import requests
import urllib3
from requests.exceptions import HTTPError
from requests.structures import CaseInsensitiveDict


import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#########################################
#	Function : set date, time to query data start, end
#########################################
def set_query_time():
    # start_from = (datetime.today() - timedelta(hours=7, minutes=5)).strftime("%Y-%m-%dT%H:%M:00.000Z")
    # to_time = (datetime.now() - timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:00.000Z")
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
                    "fixed_interval": "30s",
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
                        "match_phrase": {
                            "function": "check-rule"
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": "2022-06-24T17:00:43.791Z",
                                "lte": "2022-06-24T17:03:43.791Z",
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
#	Function : pull_request
#########################################
def pull_request(req_json):
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Basic bXNvbTpNJDBtQGRtaW4xMzI="
    url = 'https://tuc-tyb2-esdt-vip.tuc.th:9200/elf-tx-tyb-tdaa*/_search'
    proxies = {'http' :'http://proxy:80', 'https' :'https://proxy:80'}

    try:
        resp = requests.get(url=url, data=req_json, headers=headers, auth=('msom', 'M$0m@dmin132'), verify=False , proxies=proxies,allow_redirects=False)
        resp.raise_for_status()
        respdata = resp.json()
        for key, value in respdata.items():
            print(key, ":", value)

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
print("Print each key-value pair from JSON response")

#########################################
## Main Program
#########################################
req_json = set_query_time()
print(req_json)
pull_request(req_json)
