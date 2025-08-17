"""
Fixed eBay Scraper Lambda function with comprehensive error handling and rate limiting
"""

import json
import boto3
import os
import requests
import time
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

# Import shared utilities
try:
    from shared_utils import (
        get_secret, safe_decimal, get_current_timestamp, get_ttl_timestamp,
        generate_item_hash, batch_write_items, log_execution_metrics,
        RateLimiter, retry_with_backoff, clean_card_name
    )
except ImportError:
    # Fallback implementations
    def get_secret(secret_name):
        return {"client_id": "test", "client_secret": "test", "sandbox": "true"}
    
    def safe_decimal(value, default=None):
        try:
            return Decimal(str(value)) if value is not None else (default or Decimal('0'))
        except:
            return default or Decimal('0')
    
    def get_current_timestamp():
        return datetime.now(timezone.utc).isoformat()
    
    def get_ttl_timestamp(hours=24):
        return int((datetime.now(timezone.utc) + timedelta(hours=hours)).timestamp())
    
    def log_execution_metrics(name, start_time, processed=0, errors=0):
        print(f"METRICS: {name} - {time.time() - start_time:.2f}s")

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

class EbayAPIClient:
    """Enhanced eBay API client with proper error handling and rate limiting"""
    
    def __init__(self):
        self.credentials = None
        self.access_token = None
        self.token_expires = None
        self.is_sandbox = True
        
        # Rate limiter: 5000 requests per day = ~3.5 per minute
        self.rate_limiter = RateLimiter(max_requests=10, time_window=60)
        
        self._initialize_credentials()
    
    def _initialize_credentials(self):
        """Initialize eBay API credentials from Secrets Manager"""
        try:
            self.credentials = get_secret(os.environ['EBAY_CREDENTIALS_SECRET'])
            self.is_sandbox = self.credentials.get('sandbox', 'true').lower() == 'true'
            logger.info(f"eBay API initialized in {'sandbox' if self.is_sandbox else 'production'} mode")
        except Exception as e:
            logger.error(f"Failed to load eBay credentials: {str(e)}")
            raise
    
    def _get_base_urls(self):
        """Get base URLs for eBay API endpoints"""
        if self.is_sandbox:
            return {
                'oauth': 'https://api.sandbox.ebay.com/identity/v1/oauth2/token',
                'browse': 'https://api.sandbox.ebay.com/buy/browse/v1',
                'marketplace_id': 'EBAY_US'
            }
        else:
            return {
                'oauth': 'https://api.ebay.com/identity/v1/oauth2/token',
                'browse': 'https://api.ebay.com/buy/browse/v1',
                'marketplace_id': 'EBAY_US'
            }
    
    def get_access_token(self):
        """Get OAuth token for eBay API calls with caching and error handling"""
        # Check if current token is still valid
        if (self.access_token and self.token_expires and 
            datetime.now(timezone.utc) < self.token_expires):
            return self.access_token
        
        urls = self._get_base_urls()
        
        try:
            import base64
            credentials_string = f"{self.credentials['client_id']}:{self.credentials['client_secret']}"
            basic_auth = base64.b64encode(credentials_string.encode()).decode()
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f'Basic {basic_auth}',
                'User-Agent': 'CardArbitrageBot/1.0'
            }
            
            data = {
                'grant_type': 'client_credentials',
                'scope': 'https://api.ebay.com/oauth/api_scope'
            }
            
            response = requests.post(
                urls['oauth'], 
                headers=headers, 
                data=data, 
                timeout=30
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            self.access_token = token_data['access_token']
            expires_in = token_data['expires_in']
            # Set expiration 5 minutes early to be safe
            self.token_expires = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 300)
            
            logger.info("eBay OAuth token refreshed successfully")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"eBay OAuth request failed: {str(e)}")
            raise Exception(f"Failed to authenticate with eBay API: {str(e)}")
        except KeyError as e:
            logger.error(f"Missing field in OAuth response: {str(e)}")
            raise Exception(f"Invalid OAuth response from eBay: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in OAuth: {str(e)}")
            raise
    
    def search_items(self, keywords: str, category_id: str = '2536', 
                    max_price: int = 1000, limit: int = 100) -> Dict[str, Any]:
        """Search eBay items with comprehensive error handling"""
        
        # Check rate limiting
        if not self.rate_limiter.can_make_request():
            wait_time = self.rate_limiter.wait_time()
            logger.warning(f"Rate limit reached, waiting {wait_time:.1f} seconds")
            time.sleep(wait_time)
        
        try:
            token = self.get_access_token()
            urls = self._get_base_urls()
            
            url = f"{urls['browse']}/item_summary/search"
            
            headers = {
                'Authorization': f'Bearer {token}',
                'X-EBAY-C-MARKETPLACE-ID': urls['marketplace_id'],
                'User-Agent': 'CardArbitrageBot/1.0',
                'Accept': 'application/json'
            }
            
            # Build comprehensive search filters
            filters = []
            filters.append(f'price:[..{max_price}]')
            filters.append('priceCurrency:USD')
            filters.append('buyingOptions:{FIXED_PRICE,AUCTION}')
            filters.append('itemLocationCountry:US')
            filters.append('deliveryCountry:US')
            
            params = {
                'q': keywords,
                'category_ids': category_id,
                'filter': ','.join(filters),
                'sort': 'price',  # Sort by price ascending
                'limit': min(limit, 200),  # eBay max is 200
                'fieldgroups': 'MATCHING_ITEMS,EXTENDED'  # Get detailed info
            }
            
            logger.info(f"Searching eBay for '{keywords}' with max price ${max_price}")
            
            response = requests.get(
                url, 
                headers=headers, 
                params=params, 
                timeout=60  # Longer timeout for search requests
            )
            
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response structure
            if 'itemSummaries' not in data and 'errors' not in data:
                logger.warning(f"Unexpected eBay API response structure: {data}")
            
            # Handle API errors
            if 'errors' in data:
                error_messages = [error.get('message', 'Unknown error') for error in data['errors']]
                logger.error(f"eBay API errors: {error_messages}")
                raise Exception(f"eBay API returned errors: {', '.join(error_messages)}")
            
            items_count = len(data.get('itemSummaries', []))
            logger.info(f"Retrieved {items_count} items from eBay")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error("eBay API request timed out")
            raise Exception("eBay API request timed out")
        except requests.exceptions.HTTPError as e:
            logger.error(f"eBay API HTTP error: {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 429:
                raise Exception("eBay API rate limit exceeded")
            elif e.response.status_code == 401:
                # Token might be expired, clear it
                self.access_token = None
                raise Exception("eBay API authentication failed")
            else:
                raise Exception(f"eBay API error: {e.response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"eBay API request failed: {str(e)}")
            raise Exception(f"eBay API request failed: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from eBay API: {str(e)}")
            raise Exception("Invalid response from eBay API")

def process_ebay_item(item: Dict[str, Any], card_name: str, current_timestamp: str, 
                     ttl_timestamp: int) -> Optional[Dict[str, Any]]:
    """Process individual eBay item with comprehensive data extraction and validation"""
    
    try:
        # Extract basic item information
        item_id = item.get('itemId', '')
        if not item_id:
            logger.warning("Skipping item without ID")
            return None
        
        title = item.get('title', '')[:255]  # Truncate for DynamoDB
        if not title:
            logger.warning(f"Skipping item {item_id} without title")
            return None
        
        # Extract price information
        price_info = item.get('price', {})
        if not price_info or 'value' not in price_info:
            logger.warning(f"Skipping item {item_id} without price")
            return None
        
        price = safe_decimal(price_info['value'])
        currency = price_info.get('currency', 'USD')
        
        # Skip items with zero or invalid prices
        if price <= 0:
            logger.warning(f"Skipping item {item_id} with invalid price: {price}")
            return None
        
        # Extract condition
        condition = item.get('condition', 'Unknown')
        
        # Extract item URL
        item_url = item.get('itemWebUrl', '')
        
        # Extract shipping information
        shipping_cost = Decimal('0')
        shipping_type = 'Unknown'
        
        shipping_options = item.get('shippingOptions', [])
        if shipping_options:
            shipping_option = shipping_options[0]  # Use first shipping option
            shipping_cost_info = shipping_option.get('shippingCost')
            
            if shipping_cost_info and 'value' in shipping_cost_info:
                shipping_cost = safe_decimal(shipping_cost_info['value'])
            
            shipping_type = shipping_option.get('shippingCostType', 'Unknown')
            
            # Handle free shipping
            if shipping_type == 'FREE' or shipping_cost == 0:
                shipping_cost = Decimal('0')
                shipping_type = 'FREE'
        
        # Extract seller information
        seller_info = item.get('seller', {})
        seller_username = seller_info.get('username', '')
        seller_rating = safe_decimal(seller_info.get('feedbackPercentage', 0))
        seller_feedback_score = safe_decimal(seller_info.get('feedbackScore', 0))
        
        # Extract listing type
        buying_options = item.get('buyingOptions', [])
        listing_type = 'AUCTION' if 'AUCTION' in buying_options else 'FIXED_PRICE'
        
        # Calculate total cost
        total_cost = price + shipping_cost
        
        # Extract additional metadata
        thumbnail_url = ''
        if 'thumbnailImages' in item and item['thumbnailImages']:
            thumbnail_url = item['thumbnailImages'][0].get('imageUrl', '')
        
        # Extract location
        item_location = item.get('itemLocation', {})
        location_country = item_location.get('country', 'US')
        location_postal_code = item_location.get('postalCode', '')
        
        # Extract distance from buyer (if available)
        distance_from_buyer = item.get('distanceFromPickupLocation', {})
        
        # Generate unique hash for deduplication
        item_hash = generate_item_hash('ebay', item_id, card_name)
        
        # Create DynamoDB item
        listing_item = {
            'platform_card': f"ebay#{card_name.lower().replace(' ', '_')}",
            'item_id': item_id,
            'card_name': card_name,
            'platform': 'ebay',
            'title': title,
            'price': price,
            'currency': currency,
            'shipping_cost': shipping_cost,
            'shipping_type': shipping_type,
            'total_cost': total_cost,
            'condition': condition,
            'listing_url': item_url,
            'seller_username': seller_username,
            'seller_rating': seller_rating,
            'seller_feedback_score': seller_feedback_score,
            'listing_type': listing_type,
            'thumbnail_url': thumbnail_url,
            'location_country': location_country,
            'location_postal_code': location_postal_code,
            'scraped_at': current_timestamp,
            'is_active': True,
            'ttl': ttl_timestamp,
            'item_hash': item_hash
        }
        
        return listing_item
        
    except Exception as e:
        logger.error(f"Error processing eBay item {item.get('itemId', 'unknown')}: {str(e)}")
        return None

def lambda_handler(event, context):
    """Main handler for eBay scraping with comprehensive error handling"""
    start_time = time.time()
    processed_items = 0
    errors_count = 0
    
    try:
        logger.info("Starting eBay scraper")
        
        # Validate input parameters
        card_name = event.get('card_name', '').strip()
        if not card_name:
            raise ValueError("card_name is required")
        
        card_name = clean_card_name(card_name)
        max_price = safe_decimal(event.get('max_price', 1000))
        include_sold_data = event.get('include_sold_data', False)
        priority = event.get('priority', 'normal')
        
        logger.info(f"Scraping eBay for: '{card_name}', max price: ${max_price}")
        
        # Initialize eBay client
        ebay_client = EbayAPIClient()
        
        # Connect to DynamoDB
        dynamodb = boto3.resource('dynamodb')
        listings_table = dynamodb.Table(os.environ['LISTINGS_TABLE_NAME'])
        
        # Prepare timestamps
        current_timestamp = get_current_timestamp()
        ttl_timestamp = get_ttl_timestamp(24)  # 24 hours TTL
        
        # Search current listings
        search_results = retry_with_backoff(
            lambda: ebay_client.search_items(
                keywords=card_name,
                max_price=int(max_price),
                limit=100
            ),
            max_attempts=3
        )
        
        # Process items
        items_to_store = []
        
        if 'itemSummaries' in search_results:
            for item in search_results['itemSummaries']:
                processed_item = process_ebay_item(
                    item, card_name, current_timestamp, ttl_timestamp
                )
                
                if processed_item:
                    items_to_store.append(processed_item)
                    processed_items += 1
                else:
                    errors_count += 1
        
        # Store items in batches
        stored_count = 0
        if items_to_store:
            stored_count = batch_write_items(listings_table, items_to_store)
            logger.info(f"Stored {stored_count} out of {len(items_to_store)} processed items")
        
        # Optional: Search sold listings for price history
        sold_items_count = 0
        if include_sold_data and stored_count > 0:
            try:
                sold_items_count = scrape_sold_listings(ebay_client, card_name, max_price)
            except Exception as e:
                logger.warning(f"Failed to scrape sold listings: {str(e)}")
                errors_count += 1
        
        # Return success response for Step Functions
        result = {
            'statusCode': 200,
            'platform': 'ebay',
            'card_name': card_name,
            'items_found': len(search_results.get('itemSummaries', [])),
            'items_processed': processed_items,
            'items_stored': stored_count,
            'sold_items_processed': sold_items_count,
            'errors_count': errors_count,
            'timestamp': current_timestamp,
            'execution_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"eBay scraping completed successfully: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in eBay scraper: {str(e)}", exc_info=True)
        
        # Return error response for Step Functions to handle
        return {
            'statusCode': 500,
            'platform': 'ebay',
            'error': str(e),
            'error_type': type(e).__name__,
            'items_processed': processed_items,
            'errors_count': errors_count + 1,
            'timestamp': get_current_timestamp(),
            'execution_time_seconds': round(time.time() - start_time, 2)
        }
    
    finally:
        log_execution_metrics('ebay_scraper', start_time, processed_items, errors_count)

def scrape_sold_listings(ebay_client: EbayAPIClient, card_name: str, max_price: Decimal) -> int:
    """Scrape sold listings for price history analysis (optional feature)"""
    # Note: This would require eBay's Finding API or Advanced Search
    # For now, this is a placeholder for future implementation
    logger.info(f"Sold listings scraping not yet implemented for {card_name}")
    return 0

# Additional helper functions can be added here as needed