from lib import bluepipe
import sys, os
import json

client = bluepipe.BluePipe('http://demo.1stblue.cloud/api/v1////', 'etl', 'yZz6XTYSaiyx5q2u')
if __name__ == '__main__':
    query = sys.argv[1]
    result = client.search_lineage(query)
    if not result:
        print('')
    print(json.dumps([{"dstTable":x.get('dst_table'), "jobGuid":x.get('job_guid')} for x in result]))


