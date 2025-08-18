"""
Notification Lambda function
"""

import json
import boto3
import os
import logging
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

def lambda_handler(event, context):
    """Handle notifications for arbitrage opportunities"""
    
    try:
        logger.info("Processing notification request")
        
        # For initial testing, just log the notification
        opportunities_found = event.get('opportunities_found', 0)
        card_name = event.get('card_name', 'Unknown')
        
        logger.info(f"Notification: Found {opportunities_found} opportunities for {card_name}")
        
        # Mock notification response
        return {
            'statusCode': 200,
            'notifications_sent': 1 if opportunities_found > 0 else 0,
            'message': f'Processed notification for {card_name}',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in notification handler: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }