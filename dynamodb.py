import boto3
from botocore.exceptions import ClientError
from pprint import pprint
from decimal import Decimal
import time


client=boto3.client("dynamodb")

#Create DynamoDb table*************************************************************************************************************************
def create_company_table():
    table=client.create_table(
        TableName="Company", 
        KeySchema=[
          {'AttributeName':'year',
          'KeyType':'HASH' #partition key
          },
          {'AttributeName':'name',
          'KeyType':'RANGE' #sort key
          }  
        ],
        AttributeDefinitions=[
            {'AttributeName':'year',
            'AttributeType':'N' #integer
            },
            {'AttributeName':'name',
            'AttributeType':'S' #string
            }
        ],
        ProvisionedThroughput= {'ReadCapacityUnits':10,
            'WriteCapacityUnits':10 }
        
    )
    return table

#Create record in a dynamo table*******************************************************************************************************************
def put_company(name,year,HeadOffice,employees):
    response=client.put_item(
        TableName="Company",
        Item={
            'year':{
                'N':f"{year}"
            },
            'name':{
                'S':f"{name}"
            },
            'HeadOffice':{
                'S':f"{HeadOffice}"
            },
            'employees':{
                'N':f"{employees}"
            }
            
        }
    )
    return response

#Get a record from dynamodb table***************************************************************************************************************
def get_company(name, year):
    try:
        response=client.get_item(
            TableName='Company',
            Key={
                'year':{
                    'N':f"{year}"
                },
                'name':{
                    'S':f'{name}'
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

#Update a record in dynamodb table*****************************************************************************************************************
def update_company(name,year,employees,HeadOffice,team):
    response=client.update_item(
        TableName='Company',
        Key={
            'year':{
                'N':f'{year}'
            },
            'name':{
                'S':f'{name}'
            }
        },
        ExpressionAttributeNames={
            '#R':'employees',
            '#P':'HeadOffice',
            '#A':'team'
        },
        ExpressionAttributeValues={
            ':r':{
                'N':f'{employees}'
            },
            ':p':{
                'S':f'{HeadOffice}'
            },
            ':a':{
                'SS':team                                  #SS->StringSimpleList   
            }
        },
        UpdateExpression='SET #R=:r,#P=:p,#A=:a',
        ReturnValues="UPDATED_NEW"
    )
    return response

#Increment an atomic counter in dynamodb**************************************************************************************************************
def increase_employees(name,year,employee_increase):
    response=client.update_item(
        TableName='Company',
        Key={
            'year':{
                'N':f'{year}'
            },
            'name':{
                'S':f'{name}'
            }
        },
        ExpressionAttributeNames={
            '#R':'employees'
        },
        ExpressionAttributeValues={
            ':r':{
                'N':f'{employee_increase}'
            }
        },
        UpdateExpression='SET #R=#R+:r',
        ReturnValues='UPDATED_NEW'
    )
    return response

#Delete an item in dynamodb table*******************************************************************************************************************
def delete_old_company(name,year,employees):
    try:
        response=client.delete_item(
            TableName='Company',
            Key={
                'year':{
                    'N':f'{year}'
                },
                'name':{
                    'S':f'{name}'
                }
            },
            ConditionExpression='employees<=:r',
            ExpressionAttributeValues={
                ':r':{
                    'N':f'{employees}'
                }
            }
        )
    except ClientError as e:
        if e.response['Error']['Code']=='ConditionCheckeFailedException':
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response



################################################################################################################################################

if __name__=='__main__':
    #CREATE DYNAMODB------------------------------------------------------------------------------------------------------------------------
    company_table=create_company_table()
    print("create dynamodb succeded.......")
    print(f"Table status: {company_table}")

    time.sleep(30)
    
    #INSERT IN TO DYNAMODB-------------------------------------------------------------------------------------------------------------------
    company_resp=put_company("EntropikTech",2016,"Bangalore",200)
    print("insert in to dynamodb succeeded")
    pprint(company_resp, sort_dicts=False)
    

    #GET AN ITEM FROM DYNAMODB---------------------------------------------------------------------------------------------------------------------
    company=get_company('EntropikTech',2016)
    if company:
        print('Get an item from dynamodb succeeded.......')
        pprint(company, sort_dicts=False)


    #UPDATE ITEM IN DYNAMODB--------------------------------------------------------------------------------------------------------------------
    update_response=update_company("EntropikTech",2016,200,"bangalore",["SalesForce","BackEnd","FrontEnd","DevOps"])
    print("update an item in dynamodb succeeded........")
    pprint(update_response, sort_dicts=False)


    #INCREAMENT AN ATOMIC COUNTER IN DYNAMODB------------------------------------------------------------------------------------------------------
    update_response=increase_employees("EntropikTech",2016,50)
    print('Increment an atomic counter in dynamodb succeeded')
    pprint(update_response, sort_dicts=False)


    #DELETE AN ITEM IN DYNAMODB TABLE---------------------------------------------------------------------------------------------------------------
    delete_response=delete_old_company("EntropikTech",2016,260)
    if(delete_response):
        print("Delete an item dynamodb succeeded.........")
        pprint(delete_response, sort_dicts=False)