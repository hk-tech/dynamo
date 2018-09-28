from boto3.session import Session
from decimal import Decimal
import json
 
def get_dynamo_table(key_id, access_key, table_name):
    session = Session(
            aws_access_key_id=key_id,
            aws_secret_access_key=access_key,
            region_name='ap-northeast-1'
    )
 
    dynamodb = session.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    return dynamo_table
 
def insert_data_from_json(table, input_file_name):
    with open(input_file_name, "r") as f:
        json_data = json.load(f)
        with table.batch_writer() as batch:
            for record in json_data:
                record["value"] = Decimal("{}".format(record["value"])) # テーブル側で数値型を指定している場合はこのような処理が必要
                batch.put_item(Item=record)
    print('Successfully inserted data.')
 
if __name__ == '__main__':
    aws_access_key_id='idhogehoge'
    aws_secret_access_key='keyhogehoge'
    input_file_name = './sensor_data.json' 
 
    dynamo_table = get_dynamo_table(aws_access_key_id, aws_secret_access_key, 'sensor_value')
    insert_data_from_json(dynamo_table, input_file_name)
