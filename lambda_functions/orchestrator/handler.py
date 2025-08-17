# lambda_functions/orchestrator/handler.py
import boto3
import json

def lambda_handler(event, context):
    stepfunctions = boto3.client('stepfunctions')
    
    card_name = event['card_name']
    
    # Start arbitrage detection workflow
    response = stepfunctions.start_execution(
        stateMachineArn=os.environ['ARBITRAGE_STATE_MACHINE_ARN'],
        input=json.dumps({
            'card_name': card_name,
            'timestamp': context.aws_request_id
        })
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'execution_arn': response['executionArn']})
    }