# lambda_functions/arbitrage_detector/handler.py
import json
import boto3
import os
from decimal import Decimal
from datetime import datetime, timedelta

class ArbitrageDetector:
    def __init__(self):
        self.rds_data = boto3.client('rds-data')
        self.min_profit_margin = float(os.environ.get('MIN_PROFIT_MARGIN', '0.15'))
        self.max_risk_score = float(os.environ.get('MAX_RISK_SCORE', '2.0'))
        
    def detect_opportunities(self, card_name):
        """Find arbitrage opportunities for a specific card"""
        
        # Get current active listings from all platforms
        listings_response = self.rds_data.execute_statement(
            resourceArn=os.environ['DATABASE_ARN'],
            secretArn=os.environ['DATABASE_SECRET_ARN'],
            database='carddb',
            sql="""
                SELECT platform, price, shipping_cost, condition, listing_url, 
                       seller_rating, listing_type, scraped_at
                FROM listings 
                WHERE card_name = :card_name 
                  AND is_active = true 
                  AND scraped_at > NOW() - INTERVAL '2 hours'
                ORDER BY (price + shipping_cost) ASC
            """,
            parameters=[
                {'name': 'card_name', 'value': {'stringValue': card_name}}
            ]
        )
        
        listings = self._parse_rds_records(listings_response['records'])
        
        # Get historical sold prices for market validation
        sold_prices_response = self.rds_data.execute_statement(
            resourceArn=os.environ['DATABASE_ARN'],
            secretArn=os.environ['DATABASE_SECRET_ARN'],
            database='carddb',
            sql="""
                SELECT platform, price, condition, sold_date
                FROM sold_prices 
                WHERE card_name = :card_name 
                  AND sold_date > NOW() - INTERVAL '30 days'
                ORDER BY sold_date DESC
                LIMIT 100
            """,
            parameters=[
                {'name': 'card_name', 'value': {'stringValue': card_name}}
            ]
        )
        
        sold_prices = self._parse_rds_records(sold_prices_response['records'])
        
        # Find cross-platform arbitrage opportunities
        opportunities = []
        
        for buy_listing in listings:
            for sell_listing in listings:
                if (buy_listing['platform'] != sell_listing['platform'] and
                    self._conditions_compatible(buy_listing['condition'], sell_listing['condition'])):
                    
                    opportunity = self._calculate_opportunity(
                        buy_listing, sell_listing, sold_prices, card_name
                    )
                    
                    if (opportunity['profit_margin'] >= self.min_profit_margin and
                        opportunity['risk_score'] <= self.max_risk_score):
                        opportunities.append(opportunity)
        
        # Sort by profit margin descending
        opportunities.sort(key=lambda x: x['profit_margin'], reverse=True)
        
        # Store opportunities in database
        for opp in opportunities[:10]:  # Store top 10
            self._store_opportunity(opp)
        
        return opportunities
    
    def _calculate_opportunity(self, buy_listing, sell_listing, sold_prices, card_name):
        """Calculate detailed arbitrage opportunity metrics"""
        
        # Calculate total costs
        buy_total = buy_listing['price'] + buy_listing['shipping_cost']
        sell_total = sell_listing['price'] - sell_listing['shipping_cost']  # Assume buyer pays shipping
        
        # Platform fees (estimated)
        platform_fees = self._calculate_platform_fees(
            buy_listing['platform'], sell_listing['platform'], sell_total
        )
        
        # Net profit calculation
        net_sell_amount = sell_total - platform_fees
        profit_amount = net_sell_amount - buy_total
        profit_margin = profit_amount / buy_total if buy_total > 0 else 0
        
        # Risk assessment
        risk_score = self._assess_risk(buy_listing, sell_listing, sold_prices)
        
        # Market velocity (how often this card sells)
        velocity_score = self._calculate_velocity(sold_prices)
        
        return {
            'card_name': card_name,
            'buy_platform': buy_listing['platform'],
            'sell_platform': sell_listing['platform'],
            'buy_price': buy_listing['price'],
            'sell_price': sell_listing['price'],
            'buy_shipping': buy_listing['shipping_cost'],
            'sell_shipping': sell_listing['shipping_cost'],
            'buy_total': buy_total,
            'sell_total': net_sell_amount,
            'platform_fees': platform_fees,
            'profit_amount': profit_amount,
            'profit_margin': profit_margin,
            'risk_score': risk_score,
            'velocity_score': velocity_score,
            'buy_url': buy_listing['listing_url'],
            'sell_url': sell_listing['listing_url'],
            'confidence_level': self._calculate_confidence(risk_score, velocity_score)
        }
    
    def _calculate_platform_fees(self, buy_platform, sell_platform, sell_amount):
        """Estimate platform fees for selling"""
        fee_rates = {
            'ebay': 0.125,      # ~12.5% (final value fee + PayPal)
            'tcgplayer': 0.11,  # ~11% 
            'comc': 0.20,       # ~20%
            'mercari': 0.10     # ~10%
        }
        
        sell_fee_rate = fee_rates.get(sell_platform.lower(), 0.10)  # Default 10%
        return sell_amount * sell_fee_rate
    
    def _assess_risk(self, buy_listing, sell_listing, sold_prices):
        """Calculate risk score (1.0 = low risk, 3.0 = high risk)"""
        risk_score = 1.0
        
        # Seller reputation risk
        if buy_listing['seller_rating'] < 95:
            risk_score += 0.5
        if buy_listing['seller_rating'] < 90:
            risk_score += 0.5
            
        # Price volatility risk
        if sold_prices:
            recent_prices = [p['price'] for p in sold_prices[:10]]
            if len(recent_prices) > 3:
                price_std = self._calculate_std_dev(recent_prices)
                avg_price = sum(recent_prices) / len(recent_prices)
                volatility = price_std / avg_price if avg_price > 0 else 1.0
                risk_score += volatility * 2.0  # Scale volatility impact
        
        # Platform risk
        if buy_listing['platform'] == 'mercari' or sell_listing['platform'] == 'mercari':
            risk_score += 0.3  # Higher risk for less established platforms
            
        # Condition mismatch risk
        if not self._conditions_compatible(buy_listing['condition'], sell_listing['condition']):
            risk_score += 1.0
            
        return min(risk_score, 5.0)  # Cap at 5.0
    
    def _calculate_velocity(self, sold_prices):
        """Calculate how frequently this card sells"""
        if not sold_prices:
            return 0.1
            
        # Count sales in last 30 days
        recent_sales = len([p for p in sold_prices 
                           if (datetime.utcnow() - datetime.fromisoformat(p['sold_date'].replace('Z', '+00:00'))).days <= 30])
        
        # Normalize to daily velocity
        daily_velocity = recent_sales / 30.0
        return min(daily_velocity, 2.0)  # Cap at 2.0
    
    def _calculate_confidence(self, risk_score, velocity_score):
        """Calculate overall confidence level"""
        # Lower risk and higher velocity = higher confidence
        confidence = (velocity_score / risk_score) * 50
        return min(max(confidence, 0), 100)  # 0-100 scale
    
    def _conditions_compatible(self, condition1, condition2):
        """Check if card conditions are compatible for arbitrage"""
        condition_hierarchy = {
            'New': 5, 'Mint': 5, 'Near Mint': 4, 'NM': 4,
            'Lightly Played': 3, 'LP': 3, 'Light Play': 3,
            'Moderately Played': 2, 'MP': 2, 'Played': 1,
            'Heavily Played': 1, 'HP': 1, 'Poor': 0, 'Damaged': 0
        }
        
        score1 = condition_hierarchy.get(condition1, 2)
        score2 = condition_hierarchy.get(condition2, 2)
        
        # Buy condition should be equal or better than sell condition
        return score1 >= score2
    
    def _calculate_std_dev(self, numbers):
        """Calculate standard deviation"""
        if len(numbers) < 2:
            return 0
            
        mean = sum(numbers) / len(numbers)
        variance = sum((x - mean) ** 2 for x in numbers) / (len(numbers) - 1)
        return variance ** 0.5
    
    def _parse_rds_records(self, records):
        """Convert RDS Data API records to Python objects"""
        parse# AWS Cloud-Native Trading Card Arbitrage System

## Overall Architecture