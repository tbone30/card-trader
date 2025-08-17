# lambda_functions/arbitrage_detector/handler.py - DynamoDB version
import json
import boto3
import os
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr

class ArbitrageDetector:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.listings_table = self.dynamodb.Table(os.environ['LISTINGS_TABLE_NAME'])
        self.opportunities_table = self.dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
        self.min_profit_margin = Decimal(str(os.environ.get('MIN_PROFIT_MARGIN', '0.15')))
        self.max_risk_score = Decimal(str(os.environ.get('MAX_RISK_SCORE', '2.0')))
        
    def detect_opportunities(self, card_name):
        """Find arbitrage opportunities for a specific card"""
        print(f"Detecting arbitrage opportunities for: {card_name}")
        
        # Get current listings from all platforms using GSI
        try:
            response = self.listings_table.query(
                IndexName='card-name-index',
                KeyConditionExpression=Key('card_name').eq(card_name),
                FilterExpression=Attr('is_active').eq(True) & 
                               Attr('scraped_at').gt((datetime.utcnow() - timedelta(hours=2)).isoformat()),
                Limit=200
            )
            
            listings = response.get('Items', [])
            print(f"Found {len(listings)} active listings")
            
        except Exception as e:
            print(f"Error querying listings: {str(e)}")
            return []
        
        # Group listings by platform
        platform_listings = {}
        for listing in listings:
            platform = listing['platform']
            if platform not in platform_listings:
                platform_listings[platform] = []
            platform_listings[platform].append(listing)
        
        # Find cross-platform arbitrage opportunities
        opportunities = []
        platforms = list(platform_listings.keys())
        
        for i, buy_platform in enumerate(platforms):
            for j, sell_platform in enumerate(platforms):
                if i != j:  # Different platforms
                    buy_listings = platform_listings[buy_platform]
                    sell_listings = platform_listings[sell_platform]
                    
                    # Find best buy/sell combinations
                    for buy_listing in buy_listings[:20]:  # Limit to top 20 cheapest
                        for sell_listing in sell_listings[:20]:
                            if self._conditions_compatible(
                                buy_listing.get('condition', 'Unknown'), 
                                sell_listing.get('condition', 'Unknown')
                            ):
                                opportunity = self._calculate_opportunity(
                                    buy_listing, sell_listing, card_name
                                )
                                
                                if (opportunity['profit_margin'] >= self.min_profit_margin and
                                    opportunity['risk_score'] <= self.max_risk_score):
                                    opportunities.append(opportunity)
        
        # Sort by profit margin descending
        opportunities.sort(key=lambda x: float(x['profit_margin']), reverse=True)
        
        # Store top opportunities in database
        stored_count = 0
        for opp in opportunities[:10]:  # Store top 10
            try:
                self._store_opportunity(opp)
                stored_count += 1
            except Exception as e:
                print(f"Error storing opportunity: {str(e)}")
        
        print(f"Found {len(opportunities)} opportunities, stored {stored_count}")
        return opportunities
    
    def _calculate_opportunity(self, buy_listing, sell_listing, card_name):
        """Calculate detailed arbitrage opportunity metrics"""
        
        # Calculate total costs
        buy_total = buy_listing['total_cost']
        sell_price = sell_listing['price']
        
        # Platform fees (estimated)
        platform_fees = self._calculate_platform_fees(
            sell_listing['platform'], sell_price
        )
        
        # Net profit calculation
        net_sell_amount = sell_price - platform_fees
        profit_amount = net_sell_amount - buy_total
        profit_margin = profit_amount / buy_total if buy_total > 0 else Decimal('0')
        
        # Risk assessment
        risk_score = self._assess_risk(buy_listing, sell_listing)
        
        # Confidence calculation
        confidence_level = self._calculate_confidence(risk_score)
        
        current_time = datetime.utcnow().isoformat()
        expires_time = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        
        return {
            'card_name': card_name,
            'buy_platform': buy_listing['platform'],
            'sell_platform': sell_listing['platform'],
            'buy_price': buy_listing['price'],
            'sell_price': sell_listing['price'],
            'buy_shipping': buy_listing['shipping_cost'],
            'buy_total': buy_total,
            'platform_fees': platform_fees,
            'profit_amount': profit_amount,
            'profit_margin': profit_margin,
            'risk_score': risk_score,
            'confidence_level': confidence_level,
            'buy_url': buy_listing['listing_url'],
            'buy_item_id': buy_listing['item_id'],
            'sell_item_id': sell_listing['item_id'],
            'buy_condition': buy_listing.get('condition', 'Unknown'),
            'sell_condition': sell_listing.get('condition', 'Unknown'),
            'created_at': current_time,
            'expires_at': expires_time,
            'status': 'ACTIVE'
        }
    
    def _calculate_platform_fees(self, platform, sell_amount):
        """Estimate platform fees for selling"""
        fee_rates = {
            'ebay': Decimal('0.125'),      # ~12.5% (final value fee + PayPal)
            'tcgplayer': Decimal('0.11'),  # ~11% 
            'comc': Decimal('0.20'),       # ~20%
            'mercari': Decimal('0.10')     # ~10%
        }
        
        fee_rate = fee_rates.get(platform.lower(), Decimal('0.10'))
        return sell_amount * fee_rate
    
    def _assess_risk(self, buy_listing, sell_listing):
        """Calculate risk score (1.0 = low risk, 3.0 = high risk)"""
        risk_score = Decimal('1.0')
        
        # Seller reputation risk
        buy_rating = buy_listing.get('seller_rating', Decimal('100'))
        if buy_rating < 95:
            risk_score += Decimal('0.5')
        if buy_rating < 90:
            risk_score += Decimal('0.5')
            
        # Platform risk adjustment
        high_risk_platforms = ['mercari', 'facebook']
        if (buy_listing['platform'] in high_risk_platforms or 
            sell_listing['platform'] in high_risk_platforms):
            risk_score += Decimal('0.3')
            
        # Price difference risk (too good to be true?)
        price_diff = sell_listing['price'] - buy_listing['total_cost']
        if price_diff > buy_listing['total_cost']:  # 100%+ profit margin
            risk_score += Decimal('0.5')
            
        return min(risk_score, Decimal('5.0'))
    
    def _calculate_confidence(self, risk_score):
        """Calculate overall confidence level (0-100)"""
        # Lower risk = higher confidence
        base_confidence = Decimal('100') - (risk_score * Decimal('20'))
        return max(min(base_confidence, Decimal('100')), Decimal('0'))
    
    def _conditions_compatible(self, buy_condition, sell_condition):
        """Check if card conditions are compatible for arbitrage"""
        condition_hierarchy = {
            'New': 5, 'Mint': 5, 'Near Mint': 4, 'NM': 4,
            'Lightly Played': 3, 'LP': 3, 'Light Play': 3,
            'Moderately Played': 2, 'MP': 2, 'Played': 1,
            'Heavily Played': 1, 'HP': 1, 'Poor': 0, 'Damaged': 0,
            'Unknown': 2  # Default middle ground
        }
        
        buy_score = condition_hierarchy.get(buy_condition, 2)
        sell_score = condition_hierarchy.get(sell_condition, 2)
        
        # Buy condition should be equal or better than sell condition
        return buy_score >= sell_score
    
    def _store_opportunity(self, opportunity):
        """Store opportunity in DynamoDB"""
        # Add TTL (24 hours from now)
        ttl_timestamp = int((datetime.utcnow() + timedelta(hours=24)).timestamp())
        
        opportunity_item = {
            **opportunity,
            'ttl': ttl_timestamp,
            'opportunity_id': f"{opportunity['card_name']}#{opportunity['created_at']}#{opportunity['buy_platform']}#{opportunity['sell_platform']}"
        }
        
        # Convert all Decimal values for DynamoDB storage
        for key, value in opportunity_item.items():
            if isinstance(value, Decimal):
                opportunity_item[key] = value
        
        self.opportunities_table.put_item(Item=opportunity_item)

def lambda_handler(event, context):
    """Main handler for arbitrage detection"""
    try:
        detector = ArbitrageDetector()
        
        # Get card name from previous step or event
        card_name = event.get('card_name', '')
        if not card_name:
            # Try to extract from previous step results
            if isinstance(event, list) and len(event) > 0:
                card_name = event[0].get('card_name', '')
        
        if not card_name:
            raise ValueError("No card_name provided in event")
        
        print(f"Starting arbitrage detection for: {card_name}")
        
        # Detect opportunities
        opportunities = detector.detect_opportunities(card_name)
        
        # Return results for Step Functions
        return {
            'statusCode': 200,
            'card_name': card_name,
            'opportunities_found': len(opportunities),
            'top_opportunities': opportunities[:5],  # Return top 5 for notifications
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Error in arbitrage detector: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e),
            'opportunities_found': 0,
            'timestamp': datetime.utcnow().isoformat()
        }