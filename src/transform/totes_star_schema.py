import boto3
import pandas as pd
from pprint import pprint
from utils.get_bucket_names import get_data_bucket_name
import io
import json

def pull_latest_json_from_data_bucket():
    
    # bucket_name = get_data_bucket_name()
    bucket_name = "temp-test-ismail"
    file_name = "latest_db_totes.json"

    s3 = boto3.client("s3")
    
    file = s3.get_object(Bucket=bucket_name, Key=file_name)
    data = file["Body"].read().decode('utf-8')
    # data_frame = pd.read_json(io.StringIO(file['Body'].read().decode('utf-8')))
    data_list = json.loads(data)
    print(type(data_list))
    print(len(data_list))
    table_names = []
    for table in data_list:
        for key in table:
            table_names.append(key)
            
    pprint(table_names)
    # data_str = json.dumps(data_list, indent=6)
    # with open(f"data.json", "w") as outfile:
    #         outfile.write(data_str)
    # for table in data_list:
    #     pprint(table)
        # temp_dict = json.dumps(table, indent=4)
        # with open(f"{table}.json", "w") as outfile:
        #     outfile.write(temp_dict)
    # data = json.load(file)
    # pprint(file)
    # pprint(data)
    # print(type(data_dict))
    # pprint(data_list)
    
pull_latest_json_from_data_bucket()