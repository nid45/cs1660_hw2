import boto3


s3 = boto3.resource('s3', aws_access_key_id='', aws_secret_access_key='')

try:
    s3.create_bucket(Bucket='datacont-nick', CreateBucketConfiguration={
    'LocationConstraint': 'us-west-2'}) 
except:
    print("this may already exist")
    
bucket = s3.Bucket("datacont-nick") 
bucket.Acl().put(ACL='public-read')

body = open('./testfile.txt', 'rb')
o = s3.Object('datacont-nick', 'test').put(Body=body )
s3.Object('datacont-nick', 'test').Acl().put(ACL='public-read')

dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='', aws_secret_access_key='')
try:
    table = dynamodb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dynamodb.Table("DataTable")

print(table.item_count)


import csv


with open('./testfile.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',', quotechar='|')
    for item in csv_reader:
        print(item)
        ##body = open('./datafiles\\'+item[3], 'rb')
        ##s3.Object('datacont-nick', item[3]).put(Body=body)
        md = s3.Object('datacont-nick', item[3]).Acl().put(ACL='public-read')
        url = " https://s3-us-west-2.amazonaws.com/datacont-nick/"+item[3] 
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
            'description' : item[4], 'date' : item[2], 'url':url}
        try: 
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")
            
            
            
            
response = table.get_item( 
    TableName='DataTable', 
    Key={
        'PartitionKey': 'experiment2',
        'RowKey': '3'    
    }
)
item = response
print(item)

            
    
