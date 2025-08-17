# lambda_functions/api_handler/handler.py
import json
import boto3
import os
from decimal import Decimal

def lambda_handler(event, context):
    http_method = event['httpMethod']
    path = event['path']
    
    if http_method == 'GET' and path == '/opportunities':
        return get_opportunities(event)
    elif http_method == 'POST' and path == '/search':
        return trigger_search(event)
    else:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Not found'})
        }

def get_opportunities(event):
    rds_data = boto3.client('rds-data')
    
    response = rds_data.execute_statement(
        resourceArn=os.environ['DATABASE_ARN'],
        secretArn=os.environ['DATABASE_SECRET_ARN'],
        database='carddb',
        sql="""
            SELECT * FROM arbitrage_opportunities 
            WHERE profit_margin > 0.15 
            ORDER BY profit_margin DESC 
            LIMIT 50
        """
    )
    
    opportunities = []
    for record in response['records']:
        opportunities.append({
            'card_name': record[1]['stringValue'],
            'buy_price': float(record[2]['doubleValue']),
            'sell_price': float(record[3]['doubleValue']),
            'profit_margin': float(record[4]['doubleValue']),
            'buy_platform': record[5]['stringValue'],
            'sell_platform': record[6]['stringValue']
        })
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        'body': json.dumps({'opportunities': opportunities})
    }

def trigger_search(event):
    body = json.loads(event['body'])
    card_name = body['card_name']
    
    # Trigger Step Functions workflow
    stepfunctions = boto3.client('stepfunctions')
    
    stepfunctions.start_execution(
        stateMachineArn=os.environ['ARBITRAGE_STATE_MACHINE_ARN'],
        input=json.dumps({'card_name': card_name})
    )
    
    return {
        'statusCode': 202,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        'body': json.dumps({'message': 'Search initiated'})
    }