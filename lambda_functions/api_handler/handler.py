# lambda_functions/api_handler/handler.py - DynamoDB version
import json
import boto3
import os
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Custom JSON encoder for Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    http_method = event.get('httpMethod', '')
    path = event.get('path', '')
    
    try:
        if http_method == 'GET' and path == '/opportunities':
            return get_opportunities(event)
        elif http_method == 'POST' and path == '/search':
            return trigger_search(event)
        else:
            return create_response(404, {'error': 'Not found'})
    except Exception as e:
        print(f"Error in API handler: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})

def get_opportunities(event):
    """Get arbitrage opportunities from DynamoDB"""
    dynamodb = boto3.resource('dynamodb')
    opportunities_table = dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
    
    # Query parameters
    query_params = event.get('queryStringParameters') or {}
    limit = int(query_params.get('limit', 50))
    min_profit_margin = float(query_params.get('min_profit_margin', 0.15))
    
    try:
        # Use GSI to query by profit margin
        response = opportunities_table.query(
            IndexName='profit-margin-index',
            KeyConditionExpression=Key('status').eq('ACTIVE'),
            FilterExpression=Key('profit_margin').gte(min_profit_margin),
            ScanIndexForward=False,  # Sort descending
            Limit=limit
        )
        
        opportunities = []
        for item in response.get('Items', []):
            opportunity = {
                'card_name': item.get('card_name', ''),
                'buy_platform': item.get('buy_platform', ''),
                'sell_platform': item.get('sell_platform', ''),
                'buy_price': float(item.get('buy_price', 0)),
                'sell_price': float(item.get('sell_price', 0)),
                'profit_amount': float(item.get('profit_amount', 0)),
                'profit_margin': float(item.get('profit_margin', 0)),
                'risk_score': float(item.get('risk_score', 0)),
                'confidence_level': float(item.get('confidence_level', 0)),
                'buy_url': item.get('buy_url', ''),
                'created_at': item.get('created_at', ''),
                'expires_at': item.get('expires_at', '')
            }
            opportunities.append(opportunity)
        
        return create_response(200, {
            'opportunities': opportunities,
            'count': len(opportunities),
            'has_more': 'LastEvaluatedKey' in response
        })
        
    except Exception as e:
        print(f"Error querying opportunities: {str(e)}")
        return create_response(500, {'error': 'Failed to fetch opportunities'})

def trigger_search(event):
    """Trigger arbitrage search workflow"""
    try:
        body = json.loads(event.get('body', '{}'))
        card_name = body.get('card_name', '').strip()
        
        if not card_name:
            return create_response(400, {'error': 'card_name is required'})
        
        # Start Step Functions workflow
        stepfunctions = boto3.client('stepfunctions')
        
        execution_input = {
            'card_name': card_name,
            'max_price': body.get('max_price', 1000),
            'include_sold_data': body.get('include_sold_data', True),
            'requester_id': event.get('requestContext', {}).get('requestId', '')
        }
        
        response = stepfunctions.start_execution(
            stateMachineArn=os.environ['ARBITRAGE_STATE_MACHINE_ARN'],
            input=json.dumps(execution_input, cls=DecimalEncoder)
        )
        
        return create_response(202, {
            'message': 'Search initiated successfully',
            'execution_arn': response['executionArn'],
            'card_name': card_name
        })
        
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        print(f"Error triggering search: {str(e)}")
        return create_response(500, {'error': 'Failed to start search'})

def create_response(status_code, body):
    """Create standardized API response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }