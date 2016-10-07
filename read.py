#!/usr/bin/env python3

# pip3 install httplib2


import os
import requests
import json
import pprint



oauth_token = os.environ['GLOBUSONLINE']
oauth_bearer = "OAuth"

uri = 'http://awe.metagenomics.anl.gov/'

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=UTF-8',
    'Authorization': oauth_bearer + ' ' + oauth_token
}



def read(path, params):
    #print(uri+path)
    r = requests.get(uri+path, headers=headers, params=params)
    return r.json()
    
    
def read_paginated(path, params, start, end):

    limit = 50
    
    if end - start + 1 < limit:
        limit = end - start + 1
    
    position = start
    
    while position <= end:
        offset = position
        
        params['limit'] = limit
        params['offset'] = offset
        results = read("job", params)
    
        for job in results['data']:
            yield job
            position += 1
            

def getCompletedJobsPerf(start , end):
    
    params = {'query':'' , 'info.pipeline':'mgrast-prod' , 'state':'completed'}
    
    for job in read_paginated("job", params, 0, 999):
        job_id = job['id']
        #print(job_id)
        perf = read("job/"+job_id, {'perf':''})
        yield perf


    

pp = pprint.PrettyPrinter(depth=6)


results = getCompletedJobsPerf(0, 1000)
for result in results:
    if 'data' in result:
        print(result['data'])

    #pp.pprint(perf)
    
    
