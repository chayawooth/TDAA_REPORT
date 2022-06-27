

import json

# Opening JSON file
f = open('log/Python_SOM/Pull_Request_USSD.txt')

# returns JSON object as
# a dictionary
data = json.load(f)

# Iterating through the json
# list
print(data)
j=1
for d in data['hits']['hits']:
    try:
        tdaa_e2e_id= d['_source']['tdaa_e2e_id']
        msisdn=d['_source']['msisdn']
        function = d['_source']['function']
        app = d['_source']['app']
        tranCode = d['_source']['tranCode']
        tranDesc = d['_source']['tranDesc']
        process_time_ms = str(d['_source']['process_time_ms'])
        print(j,",tdaa_e2e_id:"+ tdaa_e2e_id +",msisdn:"+msisdn+",app:"+ app +",tranCode:"+ tranCode +",tranDesc:"+ tranDesc +",process_time_ms:"+ process_time_ms)
        #print(j,",tdaa_e2e_id:" + tdaa_e2e_id + ",msisdn:" + msisdn + ",function:" + function + ",tranCode:"  )
    except:
        tdaa_e2e_id = d['_source']['tdaa_e2e_id']
        msisdn = d['_source']['msisdn']
        function = d['_source']['function']
        app = d['_source']['app']
        code = str(d['_source']['code'])
        description = d['_source']['description']
        process_time_ms = str(d['_source']['process_time_ms'])
        print(j,",tdaa_e2e_id:"+ tdaa_e2e_id +",msisdn:"+msisdn+",app:"+ app +",tranCode:"+ code +",tranDesc:"+ description +",process_time_ms:"+ process_time_ms)
        #print(j, ",tdaa_e2e_id:" + tdaa_e2e_id + ",msisdn:" + msisdn + ",function:" + function + ",tranCode:")
    j=j+1


# Closing file
f.close()