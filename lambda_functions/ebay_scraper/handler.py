# lambda_functions/ebay_scraper/handler.py - DynamoDB version
import json
import boto3
import os
import requests
from datetime import datetime, timedelta
from decimal import Decimal

class EbayAPIClient:
    def __init__(self):
        self.client_id = os.environ['EBAY_CLIENT_ID']
        self.client_secret = os.environ['EBAY_CLIENT_SECRET']
        self.access_token = None
        self.token_expires = None
        
    def get_access_token(self):
        """Get OAuth token for eBay API calls"""
        if self.access_token and self.token_expires and datetime.utcnow() < self.token_expires:
            return self.access_token
            
        auth_url = "https://api.ebay.com/identity/v1/oauth2/token"
        
        import base64
        credentials = f"{self.client_id}:{self.client_secret}"
        basic_auth = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {basic_auth}'
        }
        
        data = {
            'grant_type': 'client_credentials',
            'scope': 'https://api.ebay.com/oauth/api_scope'
        }
        
        response = requests.post(auth_url, headers=headers, data=data)
        token_data = response.json()
        
        self.access_token = token_data['access_token']
        expires_in = token_data['expires_in']
        self.token_expires = datetime.utcnow() + timedelta(seconds=expires_in - 300)
        
        return self.access_token
    
    def search_items(self, keywords, category_id='2536', max_price=100, limit=50):
        """Search eBay using Browse API"""
        token = self.get_access_token()
        
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        
        headers = {
            'Authorization': f'Bearer {token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
        
        params = {
            'q': keywords,
            'category_ids': category_id,
            'filter': f'price:[..{max_price}],priceCurrency:USD,buyingOptions:{{FIXED_PRICE,AUCTION}}',
            'sort': 'price',
            'limit': limit
        }
        
        response = requests.get(url, headers=headers, params=params)
        return response.json()

def lambda_handler(event, context):
    """Main handler for eBay scraping"""
    try:
        ebay_client = EbayAPIClient()
        dynamodb = boto3.resource('dynamodb')
        listings_table = dynamodb.Table(os.environ['LISTINGS_TABLE_NAME'])
        
        # Get parameters from event
        card_name = event['card_name']
        max_price = event.get('max_price', 1000)
        
        print(f"Starting eBay scrape for: {card_name}, max price: {max_price}")
        
        # Search current listings
        current_listings = ebay_client.search_items(
            keywords=card_name,
            max_price=max_price,
            limit=100
        )
        
        processed_items = 0
        current_timestamp = datetime.utcnow().isoformat()
        ttl_timestamp = int((datetime.utcnow() + timedelta(hours=24)).timestamp())
        
        if 'itemSummaries' in current_listings:
            # Process items in batches for DynamoDB
            with listings_table.batch_writer() as batch:
                for item in current_listings['itemSummaries']:
                    try:
                        # Extract item details
                        item_id = item.get('itemId', '')
                        title = item.get('title', '')[:255]  # DynamoDB string limit
                        price = Decimal(str(item['price']['value'])) if 'price' in item else Decimal('0')
                        currency = item['price']['currency'] if 'price' in item else 'USD'
                        condition = item.get('condition', 'Unknown')
                        item_url = item.get('itemWebUrl', '')
                        
                        # Extract shipping info
                        shipping_cost = Decimal('0')
                        if 'shippingOptions' in item and item['shippingOptions']:
                            shipping_option = item['shippingOptions'][0]
                            if 'shippingCost' in shipping_option:
                                shipping_cost = Decimal(str(shipping_option['shippingCost']['value']))
                        
                        # Extract seller info
                        seller_username = item.get('seller', {}).get('username', '')
                        seller_rating = Decimal(str(item.get('seller', {}).get('feedbackPercentage', 0)))
                        
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
                            'total_cost': price + shipping_cost,
                            'condition': condition,
                            'listing_url': item_url,
                            'seller_username': seller_username,
                            'seller_rating': seller_rating,
                            'listing_type': 'FIXED_PRICE',
                            'scraped_at': current_timestamp,
                            'is_active': True,
                            'ttl': ttl_timestamp
                        }
                        
                        batch.put_item(Item=listing_item)
                        processed_items += 1
                        
                    except Exception as item_error:
                        print(f"Error processing item {item.get('itemId', 'unknown')}: {str(item_error)}")
                        continue
        
        print(f"Successfully processed {processed_items} eBay listings")
        
        # Return success response for Step Functions
        return {
            'statusCode': 200,
            'platform': 'ebay',
            'card_name': card_name,
            'items_processed': processed_items,
            'timestamp': current_timestamp
        }
        
    except Exception as e:
        print(f"Error in eBay scraper: {str(e)}")
        # Return error for Step Functions to handle
        return {
            'statusCode': 500,
            'platform': 'ebay',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }