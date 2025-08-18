import React, { useState, useEffect } from 'react';
import { 
  Search, 
  TrendingUp, 
  DollarSign, 
  AlertCircle, 
  Clock, 
  CheckCircle, 
  XCircle, 
  RefreshCw,
  ExternalLink,
  Filter,
  ArrowUpDown,
  Activity,
  Database,
  Zap
} from 'lucide-react';

const CardArbitrageDashboard = () => {
  const [opportunities, setOpportunities] = useState([]);
  const [systemHealth, setSystemHealth] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchCard, setSearchCard] = useState('');
  const [filters, setFilters] = useState({
    minProfitMargin: 0.15,
    maxRiskScore: 2.0,
    platformPair: '',
    sortBy: 'profit_margin'
  });
  const [searchLoading, setSearchLoading] = useState(false);

  // Mock API base URL - in production this would come from environment
  const API_BASE = process.env.REACT_APP_API_URL || 'https://your-api-gateway-url.amazonaws.com';

  const fetchOpportunities = async () => {
    try {
      const params = new URLSearchParams({
        min_profit_margin: filters.minProfitMargin,
        max_risk_score: filters.maxRiskScore,
        limit: 50
      });
      
      if (filters.platformPair) {
        params.append('platform_pair', filters.platformPair);
      }

      const response = await fetch(`${API_BASE}/opportunities?${params}`);
      if (!response.ok) throw new Error('Failed to fetch opportunities');
      
      const data = await response.json();
      setOpportunities(data.opportunities || []);
    } catch (err) {
      console.error('Error fetching opportunities:', err);
      // Use mock data for demo
      setOpportunities(mockOpportunities);
    }
  };

  const fetchSystemHealth = async () => {
    try {
      const response = await fetch(`${API_BASE}/health`);
      if (!response.ok) throw new Error('Health check failed');
      
      const data = await response.json();
      setSystemHealth(data);
    } catch (err) {
      console.error('Error fetching system health:', err);
      // Use mock data for demo
      setSystemHealth(mockSystemHealth);
    }
  };

  const triggerSearch = async (cardName) => {
    if (!cardName.trim()) return;
    
    setSearchLoading(true);
    try {
      const response = await fetch(`${API_BASE}/search`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          card_name: cardName,
          max_price: 1000,
          include_sold_data: true,
          priority: 'normal'
        })
      });
      
      if (!response.ok) throw new Error('Search failed');
      
      const data = await response.json();
      alert(`Search initiated for "${cardName}". Check back in a few minutes for results.`);
      setSearchCard('');
    } catch (err) {
      console.error('Search error:', err);
      alert('Search failed. Please try again.');
    } finally {
      setSearchLoading(false);
    }
  };

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      await Promise.all([fetchOpportunities(), fetchSystemHealth()]);
      setLoading(false);
    };
    
    loadData();
    
    // Refresh data every 5 minutes
    const interval = setInterval(loadData, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [filters]);

  const formatCurrency = (amount) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD'
    }).format(amount);
  };

  const formatPercentage = (decimal) => {
    return (decimal * 100).toFixed(1) + '%';
  };

  const getRiskColor = (riskScore) => {
    if (riskScore <= 1.5) return 'text-green-600 bg-green-100';
    if (riskScore <= 2.5) return 'text-yellow-600 bg-yellow-100';
    return 'text-red-600 bg-red-100';
  };

  const getPlatformIcon = (platform) => {
    switch(platform.toLowerCase()) {
      case 'ebay': return '🔵';
      case 'tcgplayer': return '🟢';
      case 'amazon': return '🟠';
      default: return '⚫';
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-indigo-900 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-12 w-12 text-blue-400 animate-spin mx-auto mb-4" />
          <p className="text-white text-lg">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-indigo-900 text-white">
      <div className="container mx-auto px-6 py-8 space-y-8">
        {/* System Status Bar */}
        <div className="bg-black/20 backdrop-blur-sm border border-white/10 rounded-xl p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                {systemHealth?.status === 'healthy' ? (
                  <CheckCircle className="h-5 w-5 text-green-400" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-400" />
                )}
                <span className="text-sm">
                  System {systemHealth?.status || 'Unknown'}
                </span>
              </div>
              <div className="text-sm text-blue-300">
                Real-time trading opportunity monitor
              </div>
            </div>
            <button
              onClick={() => window.location.reload()}
              className="p-2 bg-white/10 hover:bg-white/20 rounded-lg transition-colors"
            >
              <RefreshCw className="h-4 w-4" />
            </button>
          </div>
        </div>
        {/* System Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-blue-300 text-sm font-medium">Active Opportunities</p>
                <p className="text-3xl font-bold">{opportunities.length}</p>
              </div>
              <Activity className="h-8 w-8 text-blue-400" />
            </div>
          </div>
          
          <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-green-300 text-sm font-medium">Avg Profit Margin</p>
                <p className="text-3xl font-bold">
                  {opportunities.length > 0 
                    ? formatPercentage(opportunities.reduce((acc, opp) => acc + opp.profit_margin, 0) / opportunities.length)
                    : '0%'}
                </p>
              </div>
              <DollarSign className="h-8 w-8 text-green-400" />
            </div>
          </div>
          
          <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-purple-300 text-sm font-medium">Best Opportunity</p>
                <p className="text-3xl font-bold">
                  {opportunities.length > 0 
                    ? formatCurrency(Math.max(...opportunities.map(opp => opp.profit_amount)))
                    : '$0'}
                </p>
              </div>
              <Zap className="h-8 w-8 text-purple-400" />
            </div>
          </div>
          
          <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-orange-300 text-sm font-medium">Last Updated</p>
                <p className="text-lg font-bold">
                  {systemHealth?.timestamp ? new Date(systemHealth.timestamp).toLocaleTimeString() : 'Unknown'}
                </p>
              </div>
              <Clock className="h-8 w-8 text-orange-400" />
            </div>
          </div>
        </div>

        {/* Search Section */}
        <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
          <h2 className="text-xl font-bold mb-4 flex items-center">
            <Search className="h-5 w-5 mr-2" />
            Search for New Opportunities
          </h2>
          <div className="flex space-x-3">
            <input
              type="text"
              value={searchCard}
              onChange={(e) => setSearchCard(e.target.value)}
              placeholder="Enter card name (e.g., 'Black Lotus', 'Charizard')"
              className="flex-1 bg-white/10 border border-white/30 rounded-lg px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
              onKeyPress={(e) => e.key === 'Enter' && triggerSearch(searchCard)}
            />
            <button
              onClick={() => triggerSearch(searchCard)}
              disabled={searchLoading || !searchCard.trim()}
              className="bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700 disabled:opacity-50 px-6 py-2 rounded-lg font-medium transition-colors flex items-center space-x-2"
            >
              {searchLoading ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Search className="h-4 w-4" />
              )}
              <span>Search</span>
            </button>
          </div>
        </div>

        {/* Filters */}
        <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
          <h2 className="text-xl font-bold mb-4 flex items-center">
            <Filter className="h-5 w-5 mr-2" />
            Filters
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">Min Profit Margin</label>
              <input
                type="number"
                step="0.05"
                min="0"
                max="1"
                value={filters.minProfitMargin}
                onChange={(e) => setFilters({...filters, minProfitMargin: parseFloat(e.target.value)})}
                className="w-full bg-white/10 border border-white/30 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Max Risk Score</label>
              <input
                type="number"
                step="0.5"
                min="0"
                max="5"
                value={filters.maxRiskScore}
                onChange={(e) => setFilters({...filters, maxRiskScore: parseFloat(e.target.value)})}
                className="w-full bg-white/10 border border-white/30 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Platform Pair</label>
              <select
                value={filters.platformPair}
                onChange={(e) => setFilters({...filters, platformPair: e.target.value})}
                className="w-full bg-white/10 border border-white/30 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">All Platforms</option>
                <option value="ebay-to-tcgplayer">eBay → TCGPlayer</option>
                <option value="tcgplayer-to-ebay">TCGPlayer → eBay</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Sort By</label>
              <select
                value={filters.sortBy}
                onChange={(e) => setFilters({...filters, sortBy: e.target.value})}
                className="w-full bg-white/10 border border-white/30 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="profit_margin">Profit Margin</option>
                <option value="profit_amount">Profit Amount</option>
                <option value="confidence_level">Confidence Level</option>
              </select>
            </div>
          </div>
        </div>

        {/* Opportunities Table */}
        <div className="bg-white/10 backdrop-blur-sm rounded-xl border border-white/20 overflow-hidden">
          <div className="p-6 border-b border-white/20">
            <h2 className="text-xl font-bold flex items-center">
              <TrendingUp className="h-5 w-5 mr-2" />
              Current Arbitrage Opportunities ({opportunities.length})
            </h2>
          </div>
          
          {opportunities.length === 0 ? (
            <div className="p-8 text-center">
              <Database className="h-12 w-12 text-gray-400 mx-auto mb-4" />
              <p className="text-gray-300">No opportunities found with current filters</p>
              <p className="text-sm text-gray-400 mt-2">Try adjusting your search criteria or trigger a new search</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-black/20">
                  <tr>
                    <th className="text-left p-4 font-medium">Card Name</th>
                    <th className="text-left p-4 font-medium">Buy Platform</th>
                    <th className="text-left p-4 font-medium">Sell Platform</th>
                    <th className="text-right p-4 font-medium">Buy Price</th>
                    <th className="text-right p-4 font-medium">Sell Price</th>
                    <th className="text-right p-4 font-medium">Profit</th>
                    <th className="text-right p-4 font-medium">Margin</th>
                    <th className="text-center p-4 font-medium">Risk</th>
                    <th className="text-center p-4 font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {opportunities.slice(0, 20).map((opp, index) => (
                    <tr key={`${opp.card_name}-${opp.created_at}`} className="border-t border-white/10 hover:bg-white/5 transition-colors">
                      <td className="p-4">
                        <div>
                          <div className="font-medium">{opp.card_name}</div>
                          <div className="text-xs text-gray-400">
                            {opp.buy_condition} → {opp.sell_condition}
                          </div>
                        </div>
                      </td>
                      <td className="p-4">
                        <div className="flex items-center space-x-2">
                          <span>{getPlatformIcon(opp.buy_platform)}</span>
                          <span className="capitalize">{opp.buy_platform}</span>
                        </div>
                      </td>
                      <td className="p-4">
                        <div className="flex items-center space-x-2">
                          <span>{getPlatformIcon(opp.sell_platform)}</span>
                          <span className="capitalize">{opp.sell_platform}</span>
                        </div>
                      </td>
                      <td className="p-4 text-right font-mono">{formatCurrency(opp.buy_price)}</td>
                      <td className="p-4 text-right font-mono">{formatCurrency(opp.sell_price)}</td>
                      <td className="p-4 text-right">
                        <div className="font-mono text-green-400">
                          {formatCurrency(opp.profit_amount)}
                        </div>
                      </td>
                      <td className="p-4 text-right">
                        <div className="font-mono text-green-400">
                          {formatPercentage(opp.profit_margin)}
                        </div>
                      </td>
                      <td className="p-4 text-center">
                        <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${getRiskColor(opp.risk_score)}`}>
                          {opp.risk_score.toFixed(1)}
                        </span>
                      </td>
                      <td className="p-4 text-center">
                        {opp.buy_url && (
                          <a
                            href={opp.buy_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex items-center text-blue-400 hover:text-blue-300 transition-colors"
                          >
                            <ExternalLink className="h-4 w-4" />
                          </a>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* System Health Details */}
        {systemHealth && (
          <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
            <h2 className="text-xl font-bold mb-4 flex items-center">
              <Activity className="h-5 w-5 mr-2" />
              System Health
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div>
                <p className="text-gray-400">Status</p>
                <div className="flex items-center space-x-2">
                  {systemHealth.status === 'healthy' ? (
                    <CheckCircle className="h-4 w-4 text-green-400" />
                  ) : (
                    <XCircle className="h-4 w-4 text-red-400" />
                  )}
                  <span className="capitalize">{systemHealth.status}</span>
                </div>
              </div>
              <div>
                <p className="text-gray-400">Last Health Check</p>
                <p>{new Date(systemHealth.timestamp).toLocaleString()}</p>
              </div>
              <div>
                <p className="text-gray-400">Version</p>
                <p>{systemHealth.version || '1.0.0'}</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

// Mock data for demonstration
const mockOpportunities = [
  {
    card_name: "Black Lotus",
    buy_platform: "ebay",
    sell_platform: "tcgplayer",
    buy_price: 15000,
    sell_price: 18000,
    profit_amount: 2700,
    profit_margin: 0.18,
    risk_score: 1.2,
    confidence_level: 0.85,
    buy_condition: "Played",
    sell_condition: "Played",
    buy_url: "https://ebay.com/item/123456",
    created_at: new Date().toISOString()
  },
  {
    card_name: "Charizard Base Set",
    buy_platform: "tcgplayer",
    sell_platform: "ebay",
    buy_price: 450,
    sell_price: 580,
    profit_amount: 105,
    profit_margin: 0.23,
    risk_score: 1.8,
    confidence_level: 0.75,
    buy_condition: "Near Mint",
    sell_condition: "Near Mint",
    buy_url: "https://tcgplayer.com/product/123456",
    created_at: new Date().toISOString()
  },
  {
    card_name: "Mox Ruby",
    buy_platform: "ebay",
    sell_platform: "tcgplayer",
    buy_price: 3200,
    sell_price: 3800,
    profit_amount: 520,
    profit_margin: 0.16,
    risk_score: 1.5,
    confidence_level: 0.80,
    buy_condition: "Lightly Played",
    sell_condition: "Lightly Played",
    buy_url: "https://ebay.com/item/789012",
    created_at: new Date().toISOString()
  }
];

const mockSystemHealth = {
  status: 'healthy',
  timestamp: new Date().toISOString(),
  version: '1.0.0'
};

export default CardArbitrageDashboard;