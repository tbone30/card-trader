# lambda_functions/ebay_scraper/handler.py
import json
import boto3
import os
import requests
from datetime import datetime, timedelta

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
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {self._get_basic_auth()}'
        }
        
        data = {
            'grant_type': 'client_credentials',
            'scope': 'https://api.ebay.com/oauth/api_scope'
        }
        
        response = requests.post(auth_url, headers=headers, data=data)
        token_data = response.json()
        
        self.access_token = token_data['access_token']
        expires_in = token_data['expires_in']  # seconds
        self.token_expires = datetime.utcnow() + timedelta(seconds=expires_in - 300)  # 5min buffer
        
        return self.access_token
    
    def _get_basic_auth(self):
        """Create base64 encoded client credentials"""
        import base64
        credentials = f"{self.client_id}:{self.client_secret}"
        return base64.b64encode(credentials.encode()).decode()
    
    def search_items(self, keywords, category_id='2536', max_price=100, limit=50):
        """Search eBay using Browse API"""
        token = self.get_access_token()
        
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        
        headers = {
            'Authorization': f'Bearer {token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US',
            'X-EBAY-C-ENDUSERCTX': 'affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>'
        }
        
        params = {
            'q': keywords,
            'category_ids': category_id,
            'filter': f'price:[..{max_price}],priceCurrency:USD,buyingOptions:{FIXED_PRICE|AUCTION}',
            'sort': 'price',
            'limit': limit
        }
        
        response = requests.get(url, headers=headers, params=params)
        return response.json()
    
    def get_sold_prices(self, keywords, days_back=30):
        """Get sold/completed listings for price analysis"""
        token = self.get_access_token()
        
        # Use search with sold items filter
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        
        headers = {
            'Authorization': f'Bearer {token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        params = {
            'q': keywords,
            'category_ids': '2536',
            'filter': f'conditionIds:{1000|1500|2000|2500|3000},soldDate:[{start_date.isoformat()}..{end_date.isoformat()}]',
            'fieldgroups': 'MATCHING_ITEMS,EXTENDED',
            'limit': 200
        }
        
        response = requests.get(url, headers=headers, params=params)
        return response.json()

def lambda_handler(event, context):
    try:
        ebay_client = EbayAPIClient()
        rds_data = boto3.client('rds-data')
        
        # Get parameters from event
        card_name = event['card_name']
        max_price = event.get('max_price', 100)
        include_sold = event.get('include_sold_data', True)
        
        # Search current listings
        current_listings = ebay_client.search_items(
            keywords=card_name,
            max_price=max_price,
            limit=100
        )
        
        processed_items = 0
        
        if 'itemSummaries' in current_listings:
            for item in current_listings['itemSummaries']:
                try:
                    # Extract item details
                    item_id = item.get('itemId', '')
                    title = item.get('title', '')
                    price = float(item['price']['value']) if 'price' in item else 0.0
                    currency = item['price']['currency'] if 'price' in item else 'USD'
                    condition = item.get('condition', 'Unknown')
                    item_url = item.get('itemWebUrl', '')
                    
                    # Extract shipping info
                    shipping_cost = 0.0
                    if 'shippingOptions' in item and item['shippingOptions']:
                        shipping_option = item['shippingOptions'][0]
                        if 'shippingCost' in shipping_option:
                            shipping_cost = float(shipping_option['shippingCost']['value'])
                    
                    # Extract seller info
                    seller_username = item.get('seller', {}).get('username', '')
                    seller_rating = item.get('seller', {}).get('feedbackPercentage', 0)
                    
                    # Insert into database
                    rds_data.execute_statement(
                        resourceArn=os.environ['DATABASE_ARN'],
                        secretArn=os.environ['DATABASE_SECRET_ARN'],
                        database='carddb',
                        sql="""
                            INSERT INTO listings (
                                ebay_item_id, card_name, platform, title, price, currency,
                                shipping_cost, condition, listing_url, seller_username,
                                seller_rating, listing_type, scraped_at, is_active
                            ) VALUES (
                                :item_id, :card_name, 'ebay', :title, :price, :currency,
                                :shipping_cost, :condition, :url, :seller_username,
                                :seller_rating, :listing_type, NOW(), true
                            )
                            ON CONFLICT (ebay_item_id) DO UPDATE SET
                                price = EXCLUDED.price,
                                shipping_cost = EXCLUDED.shipping_cost,
                                scraped_at = NOW()
                        """,
                        parameters=[
                            {'name': 'item_id', 'value': {'stringValue': item_id}},
                            {'name': 'card_name', 'value': {'stringValue': card_name}},
                            {'name': 'title', 'value': {'stringValue': title}},
                            {'name': 'price', 'value': {'doubleValue': price}},
                            {'name': 'currency', 'value': {'stringValue': currency}},
                            {'name': 'shipping_cost', 'value': {'doubleValue': shipping_cost}},
                            {'name': 'condition', 'value': {'stringValue': condition}},
                            {'name': 'url', 'value': {'stringValue': item_url}},
                            {'name': 'seller_username', 'value': {'stringValue': seller_username}},
                            {'name': 'seller_rating', 'value': {'doubleValue': float(seller_rating)}},
                            {'name': 'listing_type', 'value': {'stringValue': 'FIXED_PRICE'}}
                        ]
                    )
                    processed_items += 1
                    
                except Exception as item_error:
                    print(f"Error processing item {item.get('itemId', 'unknown')}: {str(item_error)}")
                    continue
        
        # Get sold prices for market analysis
        sold_data_count = 0
        if include_sold:
            try:
                sold_listings = ebay_client.get_sold_prices(card_name, days_back=30)
                
                if 'itemSummaries' in sold_listings:
                    for sold_item in sold_listings['itemSummaries']:
                        try:
                            sold_price = float(sold_item['price']['value']) if 'price' in sold_item else 0.0
                            sold_date = sold_item.get('itemEndDate', datetime.utcnow().isoformat())
                            
                            # Insert sold price data
                            rds_data.execute_statement(
                                resourceArn=os.environ['DATABASE_ARN'],
                                secretArn=os.environ['DATABASE_SECRET_ARN'],
                                database='carddb',
                                sql="""
                                    INSERT INTO sold_prices (
                                        card_name, platform, price, condition, sold_date, title
                                    ) VALUES (
                                        :card_name, 'ebay', :price, :condition, :sold_date, :title
                                    )
                                """,
                                parameters=[
                                    {'name': 'card_name', 'value': {'stringValue': card_name}},
                                    {'name': 'price', 'value': {'doubleValue': sold_price}},
                                    {'name': 'condition', 'value': {'stringValue': sold_item.get('condition', 'Unknown')}},
                                    {'name': 'sold_date', 'value': {'stringValue': sold_date}},
                                    {'name': 'title', 'value': {'stringValue': sold_item.get('title', '')}}
                                ]
                            )
                            sold_data_count += 1
                            
                        except Exception as sold_error:
                            print(f"Error processing sold item: {str(sold_error)}")
                            continue
                            
            except Exception as sold_error:
                print(f"Error fetching sold data: {str(sold_error)}")
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'eBay data successfully processed',
                'items_processed': processed_items,
                'sold_items_processed': sold_data_count,
                'card_name': card_name
            })
        }
        
    except Exception as e:
        print(f"Error in eBay scraper: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to process eBay data',
                'details': str(e)
            })
        }