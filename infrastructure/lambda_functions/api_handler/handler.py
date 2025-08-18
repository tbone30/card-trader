"""
Fixed API Handler Lambda function with comprehensive error handling
"""

import json
import boto3
import os
import time
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Import shared utilities
try:
    from shared_utils import (
        APIError, DecimalEncoder, create_response, validate_card_name,
        get_current_timestamp, safe_decimal, safe_float, log_execution_metrics
    )
except ImportError:
    # Fallback if shared utilities aren't available
    class APIError(Exception):
        def __init__(self, message, status_code=500, error_type="InternalError"):
            self.message = message
            self.status_code = status_code
            self.error_type = error_type
            super().__init__(self.message)
    
    class DecimalEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Decimal):
                return float(obj)
            return super().default(obj)
    
    def create_response(status_code, body, headers=None):
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
    
    def validate_card_name(card_name):
        if not card_name or len(card_name.strip()) < 2:
            raise APIError("Valid card name required", 400, "BadRequest")
        return card_name.strip()
    
    def log_execution_metrics(name, start_time, processed=0, errors=0):
        print(f"METRICS: {name} - {time.time() - start_time:.2f}s, {processed} processed, {errors} errors")

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

def lambda_handler(event, context):
    """Main Lambda handler with comprehensive routing and error handling"""
    start_time = time.time()
    
    try:
        # Log incoming request
        logger.info(f"Received request: {event.get('httpMethod')} {event.get('path')}")
        
        # Extract request details
        http_method = event.get('httpMethod', '').upper()
        path = event.get('path', '').rstrip('/')
        
        # Route to appropriate handler
        if http_method == 'OPTIONS':
            return handle_options()
        elif http_method == 'GET' and path == '/health':
            return handle_health_check()
        elif http_method == 'GET' and path == '/opportunities':
            return handle_get_opportunities(event)
        elif http_method == 'POST' and path == '/search':
            return handle_trigger_search(event)
        elif http_method == 'GET' and path == '/metrics':
            return handle_get_cloudwatch_metrics(event)
        elif http_method == 'GET' and path.startswith('/opportunities/'):
            return handle_get_opportunity_details(event)
            return handle_get_opportunity_details(event)
        else:
            logger.warning(f"Unknown route: {http_method} {path}")
            return create_response(404, {
                'error': 'Not Found',
                'message': f'Route {http_method} {path} not found'
            })
    
    except APIError as e:
        logger.error(f"API Error: {e.message}")
        return create_response(e.status_code, {
            'error': e.error_type,
            'message': e.message
        })
    
    except Exception as e:
        logger.error(f"Unexpected error in API handler: {str(e)}", exc_info=True)
        return create_response(500, {
            'error': 'InternalError',
            'message': 'An unexpected error occurred'
        })
    
    finally:
        log_execution_metrics('api_handler', start_time)

def handle_options():
    """Handle CORS preflight requests"""
    return create_response(200, {}, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
        'Access-Control-Max-Age': '86400'
    })

def handle_health_check():
    """Health check endpoint"""
    try:
        # Test DynamoDB connectivity
        dynamodb = boto3.resource('dynamodb')
        opportunities_table = dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
        
        # Simple query to test connectivity
        opportunities_table.query(
            IndexName='profit-margin-index',
            KeyConditionExpression=Key('status').eq('ACTIVE'),
            Limit=1
        )
        
        return create_response(200, {
            'status': 'healthy',
            'timestamp': get_current_timestamp(),
            'version': '1.0.0'
        })
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return create_response(503, {
            'status': 'unhealthy',
            'error': 'Database connectivity issue',
            'timestamp': get_current_timestamp()
        })

def handle_get_cloudwatch_metrics(event):
    """Get CloudWatch metrics for dashboard"""
    try:
        cloudwatch = boto3.client('cloudwatch')
        lambda_client = boto3.client('lambda')
        dynamodb = boto3.client('dynamodb')
        stepfunctions = boto3.client('stepfunctions')
        
        # Get time range (last 24 hours by default)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        # Get Lambda function names from environment or discover them
        lambda_functions = []
        try:
            # List functions that match our naming pattern
            response = lambda_client.list_functions()
            for func in response.get('Functions', []):
                func_name = func['FunctionName']
                if any(pattern in func_name for pattern in ['CardArbitrageStack-', 'ApiHandler', 'EbayScraper', 'ArbitrageDetector']):
                    lambda_functions.append({
                        'name': func_name.replace('CardArbitrageStack-', '').split('-')[0],
                        'fullName': func_name,
                        'runtime': func.get('Runtime', 'unknown'),
                        'memorySize': func.get('MemorySize', 0),
                        'timeout': func.get('Timeout', 0)
                    })
        except Exception as e:
            logger.warning(f"Could not list Lambda functions: {str(e)}")
        
        # Get metrics for each Lambda function
        for func in lambda_functions:
            try:
                func_name = func['fullName']
                
                # Get invocations
                invocations = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Invocations',
                    Dimensions=[{'Name': 'FunctionName', 'Value': func_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                func['invocations'] = sum(point['Sum'] for point in invocations.get('Datapoints', []))
                
                # Get errors
                errors = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Errors',
                    Dimensions=[{'Name': 'FunctionName', 'Value': func_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                func['errors'] = sum(point['Sum'] for point in errors.get('Datapoints', []))
                
                # Get duration
                duration = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Duration',
                    Dimensions=[{'Name': 'FunctionName', 'Value': func_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Average']
                )
                durations = [point['Average'] for point in duration.get('Datapoints', [])]
                func['duration'] = sum(durations) / len(durations) if durations else 0
                
                # Get throttles
                throttles = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Throttles',
                    Dimensions=[{'Name': 'FunctionName', 'Value': func_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                func['throttles'] = sum(point['Sum'] for point in throttles.get('Datapoints', []))
                
                # Determine status
                if func['errors'] == 0 and func['throttles'] == 0:
                    func['status'] = 'healthy'
                elif func['errors'] < 5 and (func['invocations'] == 0 or (func['invocations'] - func['errors']) / func['invocations'] >= 0.9):
                    func['status'] = 'warning'
                else:
                    func['status'] = 'error'
                    
            except Exception as e:
                logger.warning(f"Could not get metrics for {func['fullName']}: {str(e)}")
                func.update({
                    'invocations': 0,
                    'errors': 0,
                    'duration': 0,
                    'throttles': 0,
                    'status': 'unknown'
                })
        
        # Get DynamoDB metrics
        dynamodb_metrics = {}
        try:
            tables = ['card-listings', 'arbitrage-opportunities']
            for table_name in tables:
                # Get consumed capacity metrics
                read_capacity = cloudwatch.get_metric_statistics(
                    Namespace='AWS/DynamoDB',
                    MetricName='ConsumedReadCapacityUnits',
                    Dimensions=[{'Name': 'TableName', 'Value': table_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                
                write_capacity = cloudwatch.get_metric_statistics(
                    Namespace='AWS/DynamoDB',
                    MetricName='ConsumedWriteCapacityUnits',
                    Dimensions=[{'Name': 'TableName', 'Value': table_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                
                # Get table item count and size (these are less frequent metrics)
                try:
                    table_info = dynamodb.describe_table(TableName=table_name)
                    item_count = table_info['Table'].get('ItemCount', 0)
                    table_size = table_info['Table'].get('TableSizeBytes', 0)
                except:
                    item_count = 0
                    table_size = 0
                
                table_key = table_name.replace('-', '_') + '_table'
                dynamodb_metrics[table_key] = {
                    'readCapacity': sum(point['Sum'] for point in read_capacity.get('Datapoints', [])),
                    'writeCapacity': sum(point['Sum'] for point in write_capacity.get('Datapoints', [])),
                    'itemCount': item_count,
                    'size': f"{table_size / (1024*1024):.1f} MB" if table_size > 0 else "0 MB"
                }
        except Exception as e:
            logger.warning(f"Could not get DynamoDB metrics: {str(e)}")
        
        # Get Step Functions metrics
        stepfunctions_metrics = {}
        try:
            # Try to find the state machine
            state_machines = stepfunctions.list_state_machines()
            card_arbitrage_sm = None
            for sm in state_machines.get('stateMachines', []):
                if 'card-arbitrage' in sm['name'].lower():
                    card_arbitrage_sm = sm['stateMachineArn']
                    break
            
            if card_arbitrage_sm:
                # Get execution metrics
                executions = stepfunctions.list_executions(
                    stateMachineArn=card_arbitrage_sm,
                    maxResults=50
                )
                
                total_executions = len(executions.get('executions', []))
                succeeded = sum(1 for ex in executions.get('executions', []) if ex['status'] == 'SUCCEEDED')
                failed = sum(1 for ex in executions.get('executions', []) if ex['status'] == 'FAILED')
                timed_out = sum(1 for ex in executions.get('executions', []) if ex['status'] == 'TIMED_OUT')
                
                stepfunctions_metrics = {
                    'executions': total_executions,
                    'succeeded': succeeded,
                    'failed': failed,
                    'timedOut': timed_out,
                    'avgDuration': 125.6  # Placeholder - calculating this would require more complex logic
                }
        except Exception as e:
            logger.warning(f"Could not get Step Functions metrics: {str(e)}")
        
        # Construct response
        response_data = {
            'lambdaFunctions': lambda_functions,
            'dynamodb': dynamodb_metrics,
            'apiGateway': {
                'requests': 0,  # Would need API Gateway CloudWatch metrics
                'errors4xx': 0,
                'errors5xx': 0,
                'latency': 0
            },
            'stepFunctions': stepfunctions_metrics,
            'lastUpdated': get_current_timestamp()
        }
        
        return create_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Error getting CloudWatch metrics: {str(e)}", exc_info=True)
        return create_response(500, {
            'error': 'MetricsError',
            'message': 'Failed to retrieve CloudWatch metrics'
        })

def handle_get_opportunities(event):
    """Get arbitrage opportunities with filtering and pagination"""
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        
        # Pagination parameters
        limit = min(int(query_params.get('limit', 50)), 100)  # Cap at 100
        last_evaluated_key = query_params.get('last_evaluated_key')
        
        # Filter parameters
        min_profit_margin = safe_decimal(query_params.get('min_profit_margin', '0.15'))
        max_risk_score = safe_decimal(query_params.get('max_risk_score', '2.0'))
        card_name = query_params.get('card_name', '').strip()
        platform_pair = query_params.get('platform_pair', '').strip()
        
        # Connect to DynamoDB
        dynamodb = boto3.resource('dynamodb')
        opportunities_table = dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
        
        # Build query parameters
        query_kwargs = {
            'IndexName': 'profit-margin-index',
            'KeyConditionExpression': Key('status').eq('ACTIVE'),
            'ScanIndexForward': False,  # Sort by profit margin descending
            'Limit': limit
        }
        
        # Add filter expressions
        filter_expressions = []
        if min_profit_margin > 0:
            filter_expressions.append(Attr('profit_margin').gte(min_profit_margin))
        if max_risk_score < 5:
            filter_expressions.append(Attr('risk_score').lte(max_risk_score))
        if card_name:
            filter_expressions.append(Attr('card_name').contains(card_name))
        if platform_pair:
            filter_expressions.append(Attr('platform_pair').eq(platform_pair))
        
        if filter_expressions:
            query_kwargs['FilterExpression'] = filter_expressions[0]
            for expr in filter_expressions[1:]:
                query_kwargs['FilterExpression'] = query_kwargs['FilterExpression'] & expr
        
        # Handle pagination
        if last_evaluated_key:
            try:
                query_kwargs['ExclusiveStartKey'] = json.loads(last_evaluated_key)
            except json.JSONDecodeError:
                raise APIError("Invalid pagination token", 400, "BadRequest")
        
        # Execute query
        response = opportunities_table.query(**query_kwargs)
        
        # Process opportunities
        opportunities = []
        for item in response.get('Items', []):
            opportunity = format_opportunity_response(item)
            opportunities.append(opportunity)
        
        # Prepare response
        result = {
            'opportunities': opportunities,
            'count': len(opportunities),
            'total_scanned': response.get('ScannedCount', 0),
            'filters_applied': {
                'min_profit_margin': float(min_profit_margin),
                'max_risk_score': float(max_risk_score),
                'card_name': card_name or None,
                'platform_pair': platform_pair or None
            }
        }
        
        # Add pagination info
        if 'LastEvaluatedKey' in response:
            result['has_more'] = True
            result['next_page_token'] = json.dumps(response['LastEvaluatedKey'], cls=DecimalEncoder)
        else:
            result['has_more'] = False
        
        return create_response(200, result)
        
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        raise APIError("Database query failed", 500, "InternalError")
    except Exception as e:
        logger.error(f"Error getting opportunities: {str(e)}")
        raise APIError("Failed to retrieve opportunities", 500, "InternalError")

def handle_get_opportunity_details(event):
    """Get details for a specific opportunity"""
    try:
        # Extract opportunity ID from path
        path_parts = event.get('path', '').split('/')
        if len(path_parts) < 3:
            raise APIError("Invalid opportunity ID", 400, "BadRequest")
        
        opportunity_id = path_parts[2]
        
        # Parse opportunity ID (format: card_name#timestamp)
        try:
            card_name, created_at = opportunity_id.split('#', 1)
            card_name = card_name.replace('%20', ' ')  # URL decode
        except ValueError:
            raise APIError("Invalid opportunity ID format", 400, "BadRequest")
        
        # Query DynamoDB
        dynamodb = boto3.resource('dynamodb')
        opportunities_table = dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
        
        response = opportunities_table.get_item(
            Key={
                'card_name': card_name,
                'created_at': created_at
            }
        )
        
        if 'Item' not in response:
            raise APIError("Opportunity not found", 404, "NotFound")
        
        opportunity = format_opportunity_response(response['Item'], include_details=True)
        
        return create_response(200, {'opportunity': opportunity})
        
    except APIError:
        raise
    except Exception as e:
        logger.error(f"Error getting opportunity details: {str(e)}")
        raise APIError("Failed to retrieve opportunity details", 500, "InternalError")

def handle_trigger_search(event):
    """Trigger arbitrage search workflow"""
    try:
        # Parse request body
        try:
            body = json.loads(event.get('body', '{}'))
        except json.JSONDecodeError:
            raise APIError("Invalid JSON in request body", 400, "BadRequest")
        
        # Validate required parameters
        card_name = validate_card_name(body.get('card_name', ''))
        
        # Optional parameters with defaults
        max_price = safe_decimal(body.get('max_price', '1000'))
        include_sold_data = body.get('include_sold_data', True)
        priority = body.get('priority', 'normal')  # normal, high
        
        # Validate parameters
        if max_price <= 0 or max_price > 10000:
            raise APIError("max_price must be between 1 and 10000", 400, "BadRequest")
        
        if priority not in ['normal', 'high']:
            raise APIError("priority must be 'normal' or 'high'", 400, "BadRequest")
        
        # Check for recent searches to prevent spam
        if not check_search_rate_limit(card_name):
            raise APIError("Search rate limit exceeded. Please wait before searching again.", 429, "RateLimitExceeded")
        
        # Prepare Step Functions input
        execution_input = {
            'card_name': card_name,
            'max_price': float(max_price),
            'include_sold_data': include_sold_data,
            'priority': priority,
            'requester_id': event.get('requestContext', {}).get('requestId', ''),
            'timestamp': get_current_timestamp()
        }
        
        # Start Step Functions execution
        stepfunctions = boto3.client('stepfunctions')
        
        try:
            response = stepfunctions.start_execution(
                stateMachineArn=os.environ['ARBITRAGE_STATE_MACHINE_ARN'],
                name=f"search-{card_name.replace(' ', '-').lower()}-{int(time.time())}",
                input=json.dumps(execution_input, cls=DecimalEncoder)
            )
            
            # Record the search request
            record_search_request(card_name, execution_input)
            
            return create_response(202, {
                'message': 'Search initiated successfully',
                'execution_arn': response['executionArn'],
                'card_name': card_name,
                'search_id': response['executionArn'].split(':')[-1],
                'estimated_completion_time': (datetime.utcnow() + timedelta(minutes=5)).isoformat()
            })
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'ExecutionLimitExceeded':
                raise APIError("Too many searches in progress. Please try again later.", 429, "RateLimitExceeded")
            else:
                logger.error(f"Step Functions error: {str(e)}")
                raise APIError("Failed to start search workflow", 500, "InternalError")
        
    except APIError:
        raise
    except Exception as e:
        logger.error(f"Error triggering search: {str(e)}")
        raise APIError("Failed to initiate search", 500, "InternalError")

def format_opportunity_response(item, include_details=False):
    """Format DynamoDB item for API response"""
    opportunity = {
        'id': f"{item.get('card_name', '')}#{item.get('created_at', '')}",
        'card_name': item.get('card_name', ''),
        'buy_platform': item.get('buy_platform', ''),
        'sell_platform': item.get('sell_platform', ''),
        'buy_price': safe_float(item.get('buy_price', 0)),
        'sell_price': safe_float(item.get('sell_price', 0)),
        'profit_amount': safe_float(item.get('profit_amount', 0)),
        'profit_margin': safe_float(item.get('profit_margin', 0)),
        'risk_score': safe_float(item.get('risk_score', 0)),
        'confidence_level': safe_float(item.get('confidence_level', 0)),
        'created_at': item.get('created_at', ''),
        'expires_at': item.get('expires_at', ''),
        'status': item.get('status', 'UNKNOWN')
    }
    
    if include_details:
        opportunity.update({
            'buy_url': item.get('buy_url', ''),
            'buy_shipping': safe_float(item.get('buy_shipping', 0)),
            'buy_total': safe_float(item.get('buy_total', 0)),
            'platform_fees': safe_float(item.get('platform_fees', 0)),
            'buy_condition': item.get('buy_condition', 'Unknown'),
            'sell_condition': item.get('sell_condition', 'Unknown'),
            'buy_item_id': item.get('buy_item_id', ''),
            'sell_item_id': item.get('sell_item_id', ''),
            'platform_pair': item.get('platform_pair', '')
        })
    
    return opportunity

def check_search_rate_limit(card_name):
    """Check if search is within rate limits (simple in-memory check)"""
    # In production, use DynamoDB or Redis for distributed rate limiting
    # For now, use simple in-memory rate limiting per Lambda container
    
    if not hasattr(check_search_rate_limit, 'search_history'):
        check_search_rate_limit.search_history = {}
    
    current_time = time.time()
    key = card_name.lower()
    
    # Clean old entries (older than 1 hour)
    check_search_rate_limit.search_history = {
        k: v for k, v in check_search_rate_limit.search_history.items()
        if current_time - v < 3600
    }
    
    # Check if card was searched recently (within 5 minutes)
    if key in check_search_rate_limit.search_history:
        if current_time - check_search_rate_limit.search_history[key] < 300:
            return False
    
    # Record this search
    check_search_rate_limit.search_history[key] = current_time
    return True

def record_search_request(card_name, search_params):
    """Record search request for analytics (optional)"""
    try:
        # This could store search requests in DynamoDB for analytics
        # For now, just log it
        logger.info(f"Search request recorded: {card_name}, params: {search_params}")
    except Exception as e:
        logger.warning(f"Failed to record search request: {str(e)}")

# Additional utility functions can be added here as needed