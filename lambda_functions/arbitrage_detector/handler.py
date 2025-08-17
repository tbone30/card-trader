"""
Fixed Arbitrage Detector Lambda function with comprehensive opportunity analysis
"""

import json
import boto3
import os
import time
import logging
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from boto3.dynamodb.conditions import Key, Attr
from itertools import combinations

# Import shared utilities
try:
    from shared_utils import (
        safe_decimal, get_current_timestamp, get_ttl_timestamp,
        calculate_platform_fees, assess_condition_compatibility,
        calculate_risk_score, calculate_confidence_level,
        batch_write_items, log_execution_metrics, DynamoDBHelper
    )
except ImportError:
    # Fallback implementations
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

class ArbitrageDetector:
    """Enhanced arbitrage detection with sophisticated analysis"""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.listings_table = self.dynamodb.Table(os.environ['LISTINGS_TABLE_NAME'])
        self.opportunities_table = self.dynamodb.Table(os.environ['OPPORTUNITIES_TABLE_NAME'])
        
        # Configuration from environment variables
        self.min_profit_margin = safe_decimal(os.environ.get('MIN_PROFIT_MARGIN', '0.15'))
        self.max_risk_score = safe_decimal(os.environ.get('MAX_RISK_SCORE', '2.0'))
        self.max_opportunities_per_card = int(os.environ.get('MAX_OPPORTUNITIES_PER_CARD', '10'))
        
        # Analysis parameters
        self.min_profit_amount = safe_decimal('5.00')  # Minimum $5 profit
        self.max_price_ratio = Decimal('0.8')  # Buy price should be ≤ 80% of sell price
        
        logger.info(f"ArbitrageDetector initialized with min_profit_margin={self.min_profit_margin}, "
                   f"max_risk_score={self.max_risk_score}")
    
    def detect_opportunities(self, card_name: str) -> List[Dict[str, Any]]:
        """
        Detect arbitrage opportunities with comprehensive analysis
        """
        logger.info(f"Detecting arbitrage opportunities for: '{card_name}'")
        
        try:
            # Get current active listings
            listings = self._get_active_listings(card_name)
            
            if not listings:
                logger.warning(f"No active listings found for '{card_name}'")
                return []
            
            logger.info(f"Found {len(listings)} active listings for analysis")
            
            # Group listings by platform for cross-platform analysis
            platform_listings = self._group_listings_by_platform(listings)
            
            # Find arbitrage opportunities
            opportunities = self._find_cross_platform_opportunities(
                platform_listings, card_name
            )
            
            # Filter and rank opportunities
            valid_opportunities = self._filter_and_rank_opportunities(opportunities)
            
            # Store top opportunities
            stored_count = self._store_opportunities(valid_opportunities[:self.max_opportunities_per_card])
            
            logger.info(f"Found {len(opportunities)} total opportunities, "
                       f"{len(valid_opportunities)} valid, {stored_count} stored")
            
            return valid_opportunities
            
        except Exception as e:
            logger.error(f"Error in detect_opportunities: {str(e)}", exc_info=True)
            raise
    
    def _get_active_listings(self, card_name: str, hours_back: int = 4) -> List[Dict[str, Any]]:
        """
        Get active listings for a card from the last few hours
        """
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
            
            response = self.listings_table.query(
                IndexName='card-name-index',
                KeyConditionExpression=Key('card_name').eq(card_name),
                FilterExpression=(
                    Attr('is_active').eq(True) & 
                    Attr('scraped_at').gt(cutoff_time) &
                    Attr('price').gt(0) &
                    Attr('total_cost').gt(0)
                ),
                Limit=500  # Reasonable limit to prevent memory issues
            )
            
            listings = response.get('Items', [])
            
            # Continue pagination if needed (up to reasonable limit)
            while 'LastEvaluatedKey' in response and len(listings) < 1000:
                response = self.listings_table.query(
                    IndexName='card-name-index',
                    KeyConditionExpression=Key('card_name').eq(card_name),
                    FilterExpression=(
                        Attr('is_active').eq(True) & 
                        Attr('scraped_at').gt(cutoff_time) &
                        Attr('price').gt(0) &
                        Attr('total_cost').gt(0)
                    ),
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    Limit=500
                )
                listings.extend(response.get('Items', []))
            
            return listings
            
        except Exception as e:
            logger.error(f"Error getting active listings: {str(e)}")
            raise
    
    def _group_listings_by_platform(self, listings: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group listings by platform and sort by price within each platform
        """
        platform_groups = {}
        
        for listing in listings:
            platform = listing.get('platform', 'unknown')
            
            if platform not in platform_groups:
                platform_groups[platform] = []
            
            platform_groups[platform].append(listing)
        
        # Sort each platform's listings by total cost (ascending for buying)
        for platform in platform_groups:
            platform_groups[platform].sort(
                key=lambda x: safe_decimal(x.get('total_cost', float('inf')))
            )
            
            logger.info(f"Platform '{platform}': {len(platform_groups[platform])} listings")
        
        return platform_groups
    
    def _find_cross_platform_opportunities(self, platform_listings: Dict[str, List[Dict[str, Any]]], 
                                          card_name: str) -> List[Dict[str, Any]]:
        """
        Find arbitrage opportunities across different platforms
        """
        opportunities = []
        platforms = list(platform_listings.keys())
        
        # Check all platform pairs
        for buy_platform, sell_platform in combinations(platforms, 2):
            # Check both directions (A->B and B->A)
            opportunities.extend(self._analyze_platform_pair(
                platform_listings[buy_platform],
                platform_listings[sell_platform],
                buy_platform,
                sell_platform,
                card_name
            ))
            
            opportunities.extend(self._analyze_platform_pair(
                platform_listings[sell_platform],
                platform_listings[buy_platform],
                sell_platform,
                buy_platform,
                card_name
            ))
        
        return opportunities
    
    def _analyze_platform_pair(self, buy_listings: List[Dict[str, Any]], 
                              sell_listings: List[Dict[str, Any]],
                              buy_platform: str, sell_platform: str, 
                              card_name: str) -> List[Dict[str, Any]]:
        """
        Analyze opportunities between two specific platforms
        """
        opportunities = []
        
        # Limit analysis to prevent excessive computation
        max_buy_items = min(50, len(buy_listings))  # Top 50 cheapest buy options
        max_sell_items = min(50, len(sell_listings))  # Top 50 sell options (should be sorted by price)
        
        for buy_listing in buy_listings[:max_buy_items]:
            for sell_listing in sell_listings[:max_sell_items]:
                
                # Skip same item (should not happen with different platforms, but be safe)
                if (buy_listing.get('item_id') == sell_listing.get('item_id') and
                    buy_listing.get('platform') == sell_listing.get('platform')):
                    continue
                
                # Quick price filter - sell price should be significantly higher than buy price
                buy_total = safe_decimal(buy_listing.get('total_cost', 0))
                sell_price = safe_decimal(sell_listing.get('price', 0))
                
                if buy_total <= 0 or sell_price <= 0:
                    continue
                
                # Minimum profit threshold check
                estimated_profit = sell_price - buy_total - calculate_platform_fees(sell_platform, sell_price)
                if estimated_profit < self.min_profit_amount:
                    continue
                
                # Price ratio check - buy price shouldn't be too close to sell price
                if buy_total > sell_price * self.max_price_ratio:
                    continue
                
                # Check condition compatibility
                if not assess_condition_compatibility(
                    buy_listing.get('condition', 'Unknown'),
                    sell_listing.get('condition', 'Unknown')
                ):
                    continue
                
                # Calculate detailed opportunity
                opportunity = self._calculate_detailed_opportunity(
                    buy_listing, sell_listing, card_name
                )
                
                if opportunity:
                    opportunities.append(opportunity)
        
        return opportunities
    
    def _calculate_detailed_opportunity(self, buy_listing: Dict[str, Any], 
                                      sell_listing: Dict[str, Any], 
                                      card_name: str) -> Optional[Dict[str, Any]]:
        """
        Calculate detailed arbitrage opportunity with comprehensive metrics
        """
        try:
            # Extract key values
            buy_price = safe_decimal(buy_listing.get('price', 0))
            buy_shipping = safe_decimal(buy_listing.get('shipping_cost', 0))
            buy_total = safe_decimal(buy_listing.get('total_cost', 0))
            sell_price = safe_decimal(sell_listing.get('price', 0))
            
            buy_platform = buy_listing.get('platform', '')
            sell_platform = sell_listing.get('platform', '')
            
            # Calculate platform fees for selling
            platform_fees = calculate_platform_fees(sell_platform, sell_price)
            
            # Calculate net amounts
            net_sell_amount = sell_price - platform_fees
            profit_amount = net_sell_amount - buy_total
            
            # Calculate profit margin
            profit_margin = profit_amount / buy_total if buy_total > 0 else Decimal('0')
            
            # Filter by minimum thresholds
            if profit_margin < self.min_profit_margin or profit_amount < self.min_profit_amount:
                return None
            
            # Calculate risk score
            risk_score = calculate_risk_score(buy_listing, sell_listing)
            
            # Filter by maximum risk
            if risk_score > self.max_risk_score:
                return None
            
            # Calculate confidence level
            confidence_level = calculate_confidence_level(risk_score)
            
            # Generate timestamps
            current_time = get_current_timestamp()
            expires_time = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
            
            # Create platform pair identifier for indexing
            platform_pair = f"{buy_platform}-to-{sell_platform}"
            
            # Create opportunity object
            opportunity = {
                'card_name': card_name,
                'buy_platform': buy_platform,
                'sell_platform': sell_platform,
                'platform_pair': platform_pair,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'buy_shipping': buy_shipping,
                'buy_total': buy_total,
                'platform_fees': platform_fees,
                'net_sell_amount': net_sell_amount,
                'profit_amount': profit_amount,
                'profit_margin': profit_margin,
                'risk_score': risk_score,
                'confidence_level': confidence_level,
                'buy_url': buy_listing.get('listing_url', ''),
                'buy_item_id': buy_listing.get('item_id', ''),
                'sell_item_id': sell_listing.get('item_id', ''),
                'buy_condition': buy_listing.get('condition', 'Unknown'),
                'sell_condition': sell_listing.get('condition', 'Unknown'),
                'buy_seller_rating': safe_decimal(buy_listing.get('seller_rating', 0)),
                'sell_seller_rating': safe_decimal(sell_listing.get('seller_rating', 0)),
                'created_at': current_time,
                'expires_at': expires_time,
                'status': 'ACTIVE',
                'analysis_version': '2.0'
            }
            
            return opportunity
            
        except Exception as e:
            logger.error(f"Error calculating opportunity: {str(e)}")
            return None
    
    def _filter_and_rank_opportunities(self, opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter and rank opportunities by quality metrics
        """
        if not opportunities:
            return []
        
        # Remove duplicates based on same buy/sell item combination
        unique_opportunities = {}
        for opp in opportunities:
            key = f"{opp['buy_item_id']}#{opp['sell_item_id']}"
            if key not in unique_opportunities:
                unique_opportunities[key] = opp
            else:
                # Keep the one with higher confidence
                existing_confidence = safe_decimal(unique_opportunities[key]['confidence_level'])
                new_confidence = safe_decimal(opp['confidence_level'])
                if new_confidence > existing_confidence:
                    unique_opportunities[key] = opp
        
        filtered_opportunities = list(unique_opportunities.values())
        
        # Create composite score for ranking
        for opp in filtered_opportunities:
            # Composite score = profit_margin * confidence_level / risk_score
            profit_margin = safe_decimal(opp['profit_margin'])
            confidence = safe_decimal(opp['confidence_level'])
            risk = max(safe_decimal(opp['risk_score']), Decimal('0.1'))  # Avoid division by zero
            
            composite_score = (profit_margin * confidence) / risk
            opp['composite_score'] = composite_score
        
        # Sort by composite score (descending)
        filtered_opportunities.sort(
            key=lambda x: safe_decimal(x['composite_score']), 
            reverse=True
        )
        
        logger.info(f"Filtered {len(opportunities)} opportunities to {len(filtered_opportunities)} unique opportunities")
        
        return filtered_opportunities
    
    def _store_opportunities(self, opportunities: List[Dict[str, Any]]) -> int:
        """
        Store opportunities in DynamoDB with proper error handling
        """
        if not opportunities:
            return 0
        
        try:
            # Prepare items for storage
            items_to_store = []
            ttl_timestamp = get_ttl_timestamp(24)  # 24 hours TTL
            
            for opp in opportunities:
                # Generate unique opportunity ID
                opportunity_id = (f"{opp['card_name']}#{opp['created_at']}#"
                                f"{opp['buy_platform']}#{opp['sell_platform']}")
                
                # Prepare DynamoDB item
                db_item = {
                    **opp,
                    'opportunity_id': opportunity_id,
                    'ttl': ttl_timestamp
                }
                
                # Ensure all Decimal values are properly formatted
                for key, value in db_item.items():
                    if isinstance(value, Decimal):
                        db_item[key] = value
                
                items_to_store.append(db_item)
            
            # Store in batches
            stored_count = batch_write_items(self.opportunities_table, items_to_store)
            
            logger.info(f"Stored {stored_count} opportunities in DynamoDB")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error storing opportunities: {str(e)}")
            return 0
    
    def get_market_insights(self, card_name: str) -> Dict[str, Any]:
        """
        Get market insights for a specific card
        """
        try:
            # Get recent opportunities
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            response = self.opportunities_table.query(
                KeyConditionExpression=Key('card_name').eq(card_name),
                FilterExpression=Attr('created_at').gt(cutoff_time),
                ScanIndexForward=False,  # Most recent first
                Limit=100
            )
            
            opportunities = response.get('Items', [])
            
            if not opportunities:
                return {
                    'card_name': card_name,
                    'total_opportunities': 0,
                    'insights': 'No recent opportunities found'
                }
            
            # Calculate market insights
            profit_margins = [safe_decimal(opp.get('profit_margin', 0)) for opp in opportunities]
            profit_amounts = [safe_decimal(opp.get('profit_amount', 0)) for opp in opportunities]
            risk_scores = [safe_decimal(opp.get('risk_score', 0)) for opp in opportunities]
            
            # Platform analysis
            platform_pairs = {}
            for opp in opportunities:
                pair = opp.get('platform_pair', 'unknown')
                if pair not in platform_pairs:
                    platform_pairs[pair] = 0
                platform_pairs[pair] += 1
            
            insights = {
                'card_name': card_name,
                'total_opportunities': len(opportunities),
                'avg_profit_margin': float(sum(profit_margins) / len(profit_margins)) if profit_margins else 0,
                'max_profit_margin': float(max(profit_margins)) if profit_margins else 0,
                'avg_profit_amount': float(sum(profit_amounts) / len(profit_amounts)) if profit_amounts else 0,
                'max_profit_amount': float(max(profit_amounts)) if profit_amounts else 0,
                'avg_risk_score': float(sum(risk_scores) / len(risk_scores)) if risk_scores else 0,
                'top_platform_pairs': dict(sorted(platform_pairs.items(), key=lambda x: x[1], reverse=True)[:5]),
                'analysis_timestamp': get_current_timestamp()
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting market insights: {str(e)}")
            return {
                'card_name': card_name,
                'error': 'Failed to analyze market data'
            }

def lambda_handler(event, context):
    """
    Main handler for arbitrage detection with comprehensive error handling
    """
    start_time = time.time()
    opportunities_found = 0
    errors_count = 0
    
    try:
        logger.info("Starting arbitrage detection")
        
        # Extract card name from event (could come from Step Functions or direct invocation)
        card_name = None
        
        if isinstance(event, dict):
            card_name = event.get('card_name', '')
        elif isinstance(event, list) and len(event) > 0:
            # From parallel Step Functions execution
            card_name = event[0].get('card_name', '') if isinstance(event[0], dict) else ''
        
        if not card_name:
            raise ValueError("No card_name provided in event")
        
        card_name = card_name.strip()
        if not card_name:
            raise ValueError("card_name cannot be empty")
        
        logger.info(f"Analyzing arbitrage opportunities for: '{card_name}'")
        
        # Initialize detector
        detector = ArbitrageDetector()
        
        # Detect opportunities
        opportunities = detector.detect_opportunities(card_name)
        opportunities_found = len(opportunities)
        
        # Get market insights
        market_insights = detector.get_market_insights(card_name)
        
        # Prepare response for Step Functions
        result = {
            'statusCode': 200,
            'card_name': card_name,
            'opportunities_found': opportunities_found,
            'top_opportunities': opportunities[:5] if opportunities else [],  # Return top 5
            'market_insights': market_insights,
            'analysis_timestamp': get_current_timestamp(),
            'execution_time_seconds': round(time.time() - start_time, 2)
        }
        
        logger.info(f"Arbitrage detection completed: found {opportunities_found} opportunities")
        return result
        
    except ValueError as e:
        logger.error(f"Input validation error: {str(e)}")
        return {
            'statusCode': 400,
            'error': str(e),
            'error_type': 'ValidationError',
            'opportunities_found': 0,
            'timestamp': get_current_timestamp()
        }
        
    except Exception as e:
        logger.error(f"Error in arbitrage detector: {str(e)}", exc_info=True)
        errors_count += 1
        
        return {
            'statusCode': 500,
            'error': str(e),
            'error_type': type(e).__name__,
            'opportunities_found': opportunities_found,
            'errors_count': errors_count,
            'timestamp': get_current_timestamp(),
            'execution_time_seconds': round(time.time() - start_time, 2)
        }
    
    finally:
        log_execution_metrics('arbitrage_detector', start_time, opportunities_found, errors_count)

# Additional utility functions for enhanced analysis
def analyze_price_trends(card_name: str, days_back: int = 7) -> Dict[str, Any]:
    """
    Analyze price trends for a card over time (future enhancement)
    """
    # Placeholder for price trend analysis
    # This could analyze historical opportunities to identify patterns
    return {
        'card_name': card_name,
        'trend': 'stable',
        'confidence': 0.5,
        'note': 'Price trend analysis not yet implemented'
    }

def detect_market_manipulation(opportunities: List[Dict[str, Any]]) -> List[str]:
    """
    Detect potential market manipulation or too-good-to-be-true opportunities
    """
    warnings = []
    
    for opp in opportunities:
        profit_margin = safe_decimal(opp.get('profit_margin', 0))
        
        # Flag extremely high profit margins
        if profit_margin > Decimal('1.0'):  # 100%+ profit margin
            warnings.append(f"Extremely high profit margin ({profit_margin:.1%}) - verify listing accuracy")
        
        # Flag very low risk with high profit
        risk_score = safe_decimal(opp.get('risk_score', 0))
        if profit_margin > Decimal('0.5') and risk_score < Decimal('1.5'):
            warnings.append("High profit with low risk - double-check listing details")
    
    return warnings