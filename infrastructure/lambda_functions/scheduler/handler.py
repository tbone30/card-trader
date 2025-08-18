"""
Scheduler Lambda function for coordinating scraping workflows
"""

import json
import boto3
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Any
from boto3.dynamodb.conditions import Key, Attr

# Import shared utilities
try:
    from shared_utils import (
        get_current_timestamp, safe_decimal, log_execution_metrics,
        DynamoDBHelper
    )
except ImportError:
    # Fallback implementations
    def get_current_timestamp():
        return datetime.now(timezone.utc).isoformat()
    
    def log_execution_metrics(name, start_time, processed=0, errors=0):
        print(f"METRICS: {name} - {time.time() - start_time:.2f}s")

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

class ArbitrageScheduler:
    """Scheduler for managing arbitrage detection workflows"""
    
    def __init__(self):
        self.stepfunctions = boto3.client('stepfunctions')
        self.dynamodb = boto3.resource('dynamodb')
        self.opportunities_table = self.dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
        self.state_machine_arn = os.environ['ARBITRAGE_STATE_MACHINE_ARN']
        
        # High-value cards to monitor regularly
        self.priority_cards = [
            "Black Lotus",
            "Ancestral Recall", 
            "Mox Ruby",
            "Mox Sapphire",
            "Mox Pearl",
            "Charizard Base Set Shadowless",
            "Pikachu Illustrator",
            "Blue-Eyes White Dragon LOB-001",
            "Dark Magician LOB-005"
        ]
        
        # Popular cards to monitor daily
        self.popular_cards = [
            "Charizard VMAX",
            "Pikachu VMAX",
            "Blue-Eyes White Dragon",
            "Dark Magician",
            "Lightning Bolt",
            "Sol Ring",
            "Mana Crypt",
            "Force of Will"
        ]
    
    def handle_scheduled_check(self, event_type: str = "hourly_check") -> Dict[str, Any]:
        """Handle scheduled opportunity checks"""
        logger.info(f"Starting scheduled check: {event_type}")
        
        results = {
            'event_type': event_type,
            'executions_started': 0,
            'cards_processed': [],
            'errors': [],
            'timestamp': get_current_timestamp()
        }
        
        try:
            if event_type == "hourly_check":
                results.update(self._handle_hourly_check())
            elif event_type == "daily_check":
                results.update(self._handle_daily_check())
            elif event_type == "priority_check":
                results.update(self._handle_priority_check())
            else:
                logger.warning(f"Unknown event type: {event_type}")
                results['errors'].append(f"Unknown event type: {event_type}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in scheduled check: {str(e)}")
            results['errors'].append(str(e))
            return results
    
    def _handle_hourly_check(self) -> Dict[str, Any]:
        """Handle hourly opportunity monitoring"""
        results = {
            'type': 'hourly_monitoring',
            'cards_checked': 0,
            'opportunities_found': 0
        }
        
        try:
            # Check for expired opportunities and clean up
            expired_count = self._cleanup_expired_opportunities()
            results['expired_opportunities_cleaned'] = expired_count
            
            # Check priority cards for new opportunities
            for card in self.priority_cards[:3]:  # Limit to 3 cards per hour to avoid overload
                try:
                    opportunities = self._check_existing_opportunities(card)
                    if len(opportunities) < 2:  # If few opportunities exist, trigger new search
                        execution_arn = self._start_arbitrage_workflow(card, priority="high")
                        if execution_arn:
                            results['executions_started'] += 1
                            results['cards_processed'].append({
                                'card_name': card,
                                'execution_arn': execution_arn,
                                'reason': 'low_opportunity_count'
                            })
                    
                    results['cards_checked'] += 1
                    results['opportunities_found'] += len(opportunities)
                    
                except Exception as e:
                    logger.error(f"Error checking card '{card}': {str(e)}")
                    results['errors'] = results.get('errors', [])
                    results['errors'].append(f"Error with {card}: {str(e)}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in hourly check: {str(e)}")
            raise
    
    def _handle_daily_check(self) -> Dict[str, Any]:
        """Handle daily comprehensive card monitoring"""
        results = {
            'type': 'daily_comprehensive',
            'cards_processed': 0,
            'total_executions': 0
        }
        
        try:
            # Process all popular cards
            all_cards = list(set(self.priority_cards + self.popular_cards))
            
            for i, card in enumerate(all_cards):
                try:
                    # Stagger executions to avoid overwhelming the system
                    if i > 0 and i % 5 == 0:
                        time.sleep(30)  # Wait 30 seconds every 5 cards
                    
                    execution_arn = self._start_arbitrage_workflow(
                        card, 
                        priority="normal",
                        include_sold_data=True
                    )
                    
                    if execution_arn:
                        results['total_executions'] += 1
                        results['cards_processed'] += 1
                        
                        logger.info(f"Started daily check for '{card}': {execution_arn}")
                    
                except Exception as e:
                    logger.error(f"Error processing card '{card}': {str(e)}")
                    results['errors'] = results.get('errors', [])
                    results['errors'].append(f"Error with {card}: {str(e)}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in daily check: {str(e)}")
            raise
    
    def _handle_priority_check(self) -> Dict[str, Any]:
        """Handle priority card deep analysis"""
        results = {
            'type': 'priority_analysis',
            'deep_analysis_cards': 0
        }
        
        try:
            # Focus on highest-value cards with comprehensive analysis
            for card in self.priority_cards[:5]:  # Top 5 priority cards
                execution_arn = self._start_arbitrage_workflow(
                    card,
                    priority="high",
                    max_price=10000,  # Higher price limit for expensive cards
                    include_sold_data=True
                )
                
                if execution_arn:
                    results['deep_analysis_cards'] += 1
                    results['cards_processed'].append({
                        'card_name': card,
                        'execution_arn': execution_arn,
                        'analysis_type': 'deep'
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in priority check: {str(e)}")
            raise
    
    def _start_arbitrage_workflow(self, card_name: str, priority: str = "normal",
                                 max_price: int = 1000, include_sold_data: bool = False) -> str:
        """Start Step Functions arbitrage workflow"""
        try:
            execution_input = {
                'card_name': card_name,
                'max_price': max_price,
                'include_sold_data': include_sold_data,
                'priority': priority,
                'scheduled': True,
                'timestamp': get_current_timestamp()
            }
            
            # Generate unique execution name
            timestamp = int(time.time())
            execution_name = f"scheduled-{card_name.replace(' ', '-').lower()}-{timestamp}"
            
            response = self.stepfunctions.start_execution(
                stateMachineArn=self.state_machine_arn,
                name=execution_name,
                input=json.dumps(execution_input)
            )
            
            return response['executionArn']
            
        except Exception as e:
            logger.error(f"Failed to start workflow for '{card_name}': {str(e)}")
            return None
    
    def _check_existing_opportunities(self, card_name: str, hours_back: int = 6) -> List[Dict[str, Any]]:
        """Check existing opportunities for a card"""
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
            
            response = self.opportunities_table.query(
                KeyConditionExpression=Key('card_name').eq(card_name),
                FilterExpression=(
                    Attr('status').eq('ACTIVE') &
                    Attr('created_at').gt(cutoff_time)
                ),
                ScanIndexForward=False,
                Limit=20
            )
            
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Error checking opportunities for '{card_name}': {str(e)}")
            return []
    
    def _cleanup_expired_opportunities(self) -> int:
        """Clean up expired opportunities (DynamoDB TTL should handle this, but manual cleanup for monitoring)"""
        try:
            current_time = datetime.now(timezone.utc).isoformat()
            cleanup_count = 0
            
            # Scan for opportunities that should be expired but might still be active
            response = self.opportunities_table.scan(
                FilterExpression=(
                    Attr('status').eq('ACTIVE') &
                    Attr('expires_at').lt(current_time)
                ),
                Limit=100  # Limit to prevent long-running operations
            )
            
            expired_opportunities = response.get('Items', [])
            
            # Mark as expired
            for opp in expired_opportunities:
                try:
                    self.opportunities_table.update_item(
                        Key={
                            'card_name': opp['card_name'],
                            'created_at': opp['created_at']
                        },
                        UpdateExpression='SET #status = :status',
                        ExpressionAttributeNames={
                            '#status': 'status'
                        },
                        ExpressionAttributeValues={
                            ':status': 'EXPIRED'
                        }
                    )
                    cleanup_count += 1
                    
                except Exception as e:
                    logger.error(f"Error updating expired opportunity: {str(e)}")
            
            if cleanup_count > 0:
                logger.info(f"Marked {cleanup_count} opportunities as expired")
            
            return cleanup_count
            
        except Exception as e:
            logger.error(f"Error in cleanup: {str(e)}")
            return 0
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health metrics"""
        try:
            health = {
                'timestamp': get_current_timestamp(),
                'status': 'healthy',
                'metrics': {}
            }
            
            # Check recent opportunities
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            response = self.opportunities_table.scan(
                FilterExpression=(
                    Attr('created_at').gt(cutoff_time) &
                    Attr('status').eq('ACTIVE')
                ),
                Select='COUNT'
            )
            
            health['metrics']['active_opportunities_24h'] = response.get('Count', 0)
            
            # Check Step Functions executions (if needed)
            # This could be extended to check recent execution status
            
            return health
            
        except Exception as e:
            logger.error(f"Error checking system health: {str(e)}")
            return {
                'timestamp': get_current_timestamp(),
                'status': 'unhealthy',
                'error': str(e)
            }

def lambda_handler(event, context):
    """Main scheduler handler"""
    start_time = time.time()
    
    try:
        logger.info(f"Scheduler invoked with event: {event}")
        
        scheduler = ArbitrageScheduler()
        
        # Determine event type
        event_type = "hourly_check"  # Default
        
        if isinstance(event, dict):
            event_type = event.get('type', event.get('event_type', 'hourly_check'))
            
            # Handle different event sources
            if 'source' in event and event['source'] == 'aws.events':
                # EventBridge scheduled event
                event_type = event.get('detail', {}).get('type', 'hourly_check')
            elif 'Records' in event:
                # SQS event
                event_type = 'queue_processing'
        
        # Process the scheduled event
        if event_type == 'queue_processing':
            result = handle_queue_messages(event)
        elif event_type == 'health_check':
            result = scheduler.get_system_health()
        else:
            result = scheduler.handle_scheduled_check(event_type)
        
        logger.info(f"Scheduler completed successfully: {result}")
        return {
            'statusCode': 200,
            'body': json.dumps(result),
            'execution_time_seconds': round(time.time() - start_time, 2)
        }
        
    except Exception as e:
        logger.error(f"Error in scheduler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': get_current_timestamp()
            }),
            'execution_time_seconds': round(time.time() - start_time, 2)
        }
    
    finally:
        log_execution_metrics('scheduler', start_time)

def handle_queue_messages(event) -> Dict[str, Any]:
    """Handle SQS queue messages (future enhancement)"""
    results = {
        'messages_processed': 0,
        'errors': []
    }
    
    for record in event.get('Records', []):
        try:
            # Process individual SQS messages
            # This could handle manual card search requests, etc.
            message_body = json.loads(record.get('body', '{}'))
            logger.info(f"Processing queue message: {message_body}")
            results['messages_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing queue message: {str(e)}")
            results['errors'].append(str(e))
    
    return results