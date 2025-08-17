"""
Shared utilities for Card Arbitrage Lambda functions
"""

import json
import boto3
import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
from botocore.exceptions import ClientError
import time
import hashlib

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for Decimal objects"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

class APIError(Exception):
    """Custom exception for API errors"""
    def __init__(self, message: str, status_code: int = 500, error_type: str = "InternalError"):
        self.message = message
        self.status_code = status_code
        self.error_type = error_type
        super().__init__(self.message)

def get_secret(secret_name: str) -> Dict[str, Any]:
    """
    Retrieve secrets from AWS Secrets Manager with caching
    """
    try:
        # Simple in-memory cache for secrets (Lambda container reuse)
        if not hasattr(get_secret, 'cache'):
            get_secret.cache = {}
        
        # Check cache first (valid for 5 minutes)
        cache_key = secret_name
        if cache_key in get_secret.cache:
            cached_secret, timestamp = get_secret.cache[cache_key]
            if time.time() - timestamp < 300:  # 5 minutes
                return cached_secret
        
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        
        # Cache the secret
        get_secret.cache[cache_key] = (secret_data, time.time())
        
        return secret_data
        
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
        raise APIError(f"Failed to retrieve credentials", 500, "ConfigurationError")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in secret {secret_name}: {str(e)}")
        raise APIError(f"Invalid credential format", 500, "ConfigurationError")

def safe_decimal(value: Union[str, int, float], default: Decimal = Decimal('0')) -> Decimal:
    """
    Safely convert a value to Decimal with proper error handling
    """
    if value is None:
        return default
    
    try:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except (ValueError, TypeError, OverflowError):
        logger.warning(f"Could not convert {value} to Decimal, using default {default}")
        return default

def safe_float(value: Union[str, int, float, Decimal], default: float = 0.0) -> float:
    """
    Safely convert a value to float with proper error handling
    """
    if value is None:
        return default
    
    try:
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
    except (ValueError, TypeError, OverflowError):
        logger.warning(f"Could not convert {value} to float, using default {default}")
        return default

def clean_card_name(card_name: str) -> str:
    """
    Clean and normalize card name for consistent processing
    """
    if not card_name:
        return ""
    
    # Remove extra whitespace and normalize
    cleaned = ' '.join(card_name.strip().split())
    
    # Remove special characters that might cause issues
    cleaned = cleaned.replace('"', '').replace("'", "")
    
    # Limit length
    if len(cleaned) < 2:
        raise APIError("Card name must be at least 2 characters", 400, "BadRequest")
    
    return cleaned

def get_current_timestamp() -> str:
    """Get current UTC timestamp in ISO format"""
    return datetime.now(timezone.utc).isoformat()

def get_ttl_timestamp(hours: int = 24) -> int:
    """Get TTL timestamp for DynamoDB (Unix timestamp)"""
    expiry_time = datetime.now(timezone.utc) + timedelta(hours=hours)
    return int(expiry_time.timestamp())

def generate_item_hash(platform: str, item_id: str, card_name: str) -> str:
    """Generate consistent hash for item identification"""
    data = f"{platform}#{item_id}#{card_name}".lower()
    return hashlib.md5(data.encode()).hexdigest()[:16]

def retry_with_backoff(func, max_attempts: int = 3, base_delay: float = 1.0):
    """
    Retry function with exponential backoff
    """
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
            time.sleep(delay)

def batch_write_items(table, items: List[Dict], batch_size: int = 25):
    """
    Write items to DynamoDB in batches with error handling
    """
    if not items:
        return 0
    
    written_count = 0
    
    # Process in batches
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        try:
            with table.batch_writer() as batch_writer:
                for item in batch:
                    batch_writer.put_item(Item=item)
            written_count += len(batch)
            
        except Exception as e:
            logger.error(f"Failed to write batch {i//batch_size + 1}: {str(e)}")
            # Try individual writes for failed batch
            for item in batch:
                try:
                    table.put_item(Item=item)
                    written_count += 1
                except Exception as item_error:
                    logger.error(f"Failed to write individual item: {str(item_error)}")
    
    return written_count

def calculate_platform_fees(platform: str, amount: Decimal) -> Decimal:
    """
    Calculate estimated platform fees for selling
    """
    fee_rates = {
        'ebay': Decimal('0.125'),        # ~12.5% (final value fee + payment processing)
        'tcgplayer': Decimal('0.11'),    # ~11% 
        'comc': Decimal('0.20'),         # ~20%
        'mercari': Decimal('0.10'),      # ~10%
        'facebook': Decimal('0.05'),     # ~5% (Facebook Marketplace)
        'cardmarket': Decimal('0.08'),   # ~8%
        'default': Decimal('0.10')       # Default 10%
    }
    
    fee_rate = fee_rates.get(platform.lower(), fee_rates['default'])
    return (amount * fee_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def assess_condition_compatibility(buy_condition: str, sell_condition: str) -> bool:
    """
    Check if card conditions are compatible for arbitrage
    """
    condition_hierarchy = {
        'gem mint': 10, 'pristine': 10, 'black label': 10,
        'mint': 9, 'perfect': 9, 'psa 10': 10, 'bgs 10': 10,
        'near mint': 8, 'nm': 8, 'nm/mint': 8, 'psa 9': 9, 'bgs 9': 9,
        'excellent': 7, 'ex': 7, 'psa 8': 8, 'bgs 8': 8,
        'very good': 6, 'vg': 6, 'psa 7': 7, 'bgs 7': 7,
        'good': 5, 'gd': 5, 'psa 6': 6, 'bgs 6': 6,
        'lightly played': 4, 'lp': 4, 'light play': 4, 'psa 5': 5,
        'moderately played': 3, 'mp': 3, 'played': 3, 'psa 4': 4,
        'heavily played': 2, 'hp': 2, 'psa 3': 3,
        'damaged': 1, 'dmg': 1, 'poor': 1, 'psa 2': 2, 'psa 1': 1,
        'unknown': 4, 'ungraded': 4, '': 4  # Default middle ground
    }
    
    buy_score = condition_hierarchy.get(buy_condition.lower().strip(), 4)
    sell_score = condition_hierarchy.get(sell_condition.lower().strip(), 4)
    
    # Buy condition should be equal or better than sell condition
    # Allow 1-point tolerance for grading variations
    return buy_score >= (sell_score - 1)

def calculate_risk_score(buy_listing: Dict, sell_listing: Dict) -> Decimal:
    """
    Calculate comprehensive risk score for arbitrage opportunity
    """
    risk_score = Decimal('1.0')  # Base risk
    
    # Seller reputation risk
    buy_rating = safe_decimal(buy_listing.get('seller_rating', 100))
    if buy_rating < 95:
        risk_score += Decimal('0.3')
    if buy_rating < 90:
        risk_score += Decimal('0.5')
    if buy_rating < 85:
        risk_score += Decimal('0.7')
    
    # Platform risk adjustment
    high_risk_platforms = ['mercari', 'facebook', 'craigslist', 'offerup']
    medium_risk_platforms = ['comc', 'cardmarket']
    
    buy_platform = buy_listing.get('platform', '').lower()
    sell_platform = sell_listing.get('platform', '').lower()
    
    if buy_platform in high_risk_platforms or sell_platform in high_risk_platforms:
        risk_score += Decimal('0.4')
    elif buy_platform in medium_risk_platforms or sell_platform in medium_risk_platforms:
        risk_score += Decimal('0.2')
    
    # Price difference risk (too good to be true?)
    buy_total = safe_decimal(buy_listing.get('total_cost', 0))
    sell_price = safe_decimal(sell_listing.get('price', 0))
    
    if buy_total > 0:
        profit_margin = (sell_price - buy_total) / buy_total
        if profit_margin > 1:  # 100%+ profit margin
            risk_score += Decimal('0.8')
        elif profit_margin > 0.5:  # 50%+ profit margin
            risk_score += Decimal('0.4')
    
    # Listing age risk (very new listings might be errors)
    try:
        buy_scraped = datetime.fromisoformat(buy_listing.get('scraped_at', ''))
        hours_since_scraped = (datetime.now(timezone.utc) - buy_scraped).total_seconds() / 3600
        if hours_since_scraped < 1:  # Very new listing
            risk_score += Decimal('0.2')
    except (ValueError, TypeError):
        risk_score += Decimal('0.1')  # Unknown scraping time adds small risk
    
    # Condition mismatch risk
    buy_condition = buy_listing.get('condition', 'Unknown')
    sell_condition = sell_listing.get('condition', 'Unknown')
    if not assess_condition_compatibility(buy_condition, sell_condition):
        risk_score += Decimal('1.0')  # Major risk if conditions don't match
    
    # Cap risk score at 5.0
    return min(risk_score, Decimal('5.0'))

def calculate_confidence_level(risk_score: Decimal) -> Decimal:
    """
    Calculate confidence level based on risk score (0-100)
    """
    # Higher risk = lower confidence
    # Risk score of 1.0 = 90% confidence, 3.0 = 50% confidence, 5.0 = 10% confidence
    base_confidence = Decimal('100') - ((risk_score - Decimal('1.0')) * Decimal('20'))
    return max(min(base_confidence, Decimal('100')), Decimal('10'))

class DynamoDBHelper:
    """Helper class for common DynamoDB operations"""
    
    def __init__(self, table_name: str):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
    
    def put_item_safe(self, item: Dict[str, Any]) -> bool:
        """Safely put item to DynamoDB with error handling"""
        try:
            self.table.put_item(Item=item)
            return True
        except Exception as e:
            logger.error(f"Failed to put item to DynamoDB: {str(e)}")
            return False
    
    def query_safe(self, **kwargs) -> Dict[str, Any]:
        """Safely query DynamoDB with error handling"""
        try:
            return self.table.query(**kwargs)
        except Exception as e:
            logger.error(f"Failed to query DynamoDB: {str(e)}")
            return {'Items': [], 'Count': 0}
    
    def scan_safe(self, **kwargs) -> Dict[str, Any]:
        """Safely scan DynamoDB with error handling"""
        try:
            return self.table.scan(**kwargs)
        except Exception as e:
            logger.error(f"Failed to scan DynamoDB: {str(e)}")
            return {'Items': [], 'Count': 0}

def log_execution_metrics(function_name: str, start_time: float, 
                         items_processed: int = 0, errors_count: int = 0):
    """
    Log execution metrics for monitoring
    """
    execution_time = time.time() - start_time
    
    logger.info(f"METRICS: {function_name} executed in {execution_time:.2f}s, "
               f"processed {items_processed} items, {errors_count} errors")
    
    # Add custom metrics for CloudWatch if needed
    try:
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='CardArbitrage',
            MetricData=[
                {
                    'MetricName': 'ExecutionTime',
                    'Value': execution_time,
                    'Unit': 'Seconds',
                    'Dimensions': [
                        {
                            'Name': 'Function',
                            'Value': function_name
                        }
                    ]
                },
                {
                    'MetricName': 'ItemsProcessed',
                    'Value': items_processed,
                    'Unit': 'Count',
                    'Dimensions': [
                        {
                            'Name': 'Function', 
                            'Value': function_name
                        }
                    ]
                }
            ]
        )
    except Exception as e:
        logger.warning(f"Failed to send metrics to CloudWatch: {str(e)}")

# Rate limiting helper
class RateLimiter:
    """Simple in-memory rate limiter for API calls"""
    
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    def can_make_request(self) -> bool:
        """Check if we can make a request within rate limits"""
        current_time = time.time()
        
        # Remove old requests outside the time window
        self.requests = [req_time for req_time in self.requests 
                        if current_time - req_time < self.time_window]
        
        # Check if we can make another request
        if len(self.requests) < self.max_requests:
            self.requests.append(current_time)
            return True
        
        return False
    
    def wait_time(self) -> float:
        """Get time to wait before next request"""
        if not self.requests:
            return 0
        oldest_request = min(self.requests)
        wait_time = self.time_window - (time.time() - oldest_request)
        return max(0, wait_time)

def create_response(status_code: int, body: Dict[str, Any], 
                   headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Create standardized API Gateway response
    """
    default_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
        'Content-Type': 'application/json'
    }
    
    if headers:
        default_headers.update(headers)
    
    return {
        'statusCode': status_code,
        'headers': default_headers,
        'body': json.dumps(body, cls=DecimalEncoder)
    }

def validate_card_name(card_name: str) -> str:
    """
    Validate card name input
    """
    if not card_name or not isinstance(card_name, str):
        raise APIError("Card name is required", 400, "BadRequest")
    
    cleaned = clean_card_name(card_name)
    if not cleaned:
        raise APIError("Valid card name is required", 400, "BadRequest")
    
    if len(cleaned) > 255:
        cleaned = cleaned[:255]
        logger.warning(f"Card name truncated to 255 characters: {cleaned}")
    return cleaned