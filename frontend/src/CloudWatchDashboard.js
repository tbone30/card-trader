import React, { useState, useEffect } from 'react';
import { 
  Activity, 
  TrendingUp, 
  AlertTriangle, 
  Clock, 
  Database,
  Zap,
  Server,
  BarChart3,
  RefreshCw,
  ExternalLink,
  CheckCircle,
  XCircle,
  AlertCircle
} from 'lucide-react';

const CloudWatchDashboard = () => {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  // API base URL - should match your other components
  const API_BASE = process.env.REACT_APP_API_URL || 'https://your-api-gateway-url.amazonaws.com';

  const fetchMetrics = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_BASE}/metrics`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      setMetrics(data);
      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      console.error('Failed to fetch CloudWatch metrics:', err);
      setError(`Failed to fetch CloudWatch metrics: ${err.message}`);
      
      // Fallback to mock data if API fails
      const mockMetrics = {
        lambdaFunctions: [
          {
            name: 'ApiHandler',
            fullName: 'CardArbitrageStack-ApiHandler-ABC123',
            invocations: 245,
            errors: 2,
            duration: 156.7,
            throttles: 0,
            status: 'healthy'
          },
          {
            name: 'EbayScraper',
            fullName: 'CardArbitrageStack-EbayScraper-DEF456',
            invocations: 48,
            errors: 1,
            duration: 4250.3,
            throttles: 0,
            status: 'warning'
          },
          {
            name: 'ArbitrageDetector',
            fullName: 'CardArbitrageStack-ArbitrageDetector-GHI789',
            invocations: 32,
            errors: 0,
            duration: 890.2,
            throttles: 0,
            status: 'healthy'
          },
          {
            name: 'NotificationHandler',
            fullName: 'CardArbitrageStack-NotificationHandler-JKL012',
            invocations: 15,
            errors: 0,
            duration: 245.8,
            throttles: 0,
            status: 'healthy'
          },
          {
            name: 'SchedulerLambda',
            fullName: 'CardArbitrageStack-SchedulerLambda-MNO345',
            invocations: 72,
            errors: 0,
            duration: 123.4,
            throttles: 0,
            status: 'healthy'
          }
        ],
        dynamodb: {
          listings_table: {
            readCapacity: 125.5,
            writeCapacity: 89.2,
            itemCount: 1247,
            size: '45.2 MB'
          },
          arbitrage_opportunities_table: {
            readCapacity: 67.3,
            writeCapacity: 23.1,
            itemCount: 89,
            size: '8.7 MB'
          }
        },
        apiGateway: {
          requests: 312,
          errors4xx: 5,
          errors5xx: 1,
          latency: 187.3
        },
        stepFunctions: {
          executions: 24,
          succeeded: 22,
          failed: 1,
          timedOut: 1,
          avgDuration: 125.6
        }
      };
      setMetrics(mockMetrics);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 60000); // Refresh every minute
    return () => clearInterval(interval);
  }, []);

  const getStatusIcon = (status) => {
    switch (status) {
      case 'healthy':
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'warning':
        return <AlertCircle className="w-4 h-4 text-yellow-500" />;
      case 'error':
        return <XCircle className="w-4 h-4 text-red-500" />;
      default:
        return <AlertTriangle className="w-4 h-4 text-gray-500" />;
    }
  };

  const getSuccessRate = (invocations, errors) => {
    if (invocations === 0) return 100;
    return ((invocations - errors) / invocations * 100).toFixed(1);
  };

  const openCloudWatchConsole = () => {
    const region = 'us-east-1'; // Adjust based on your deployment region
    const url = `https://${region}.console.aws.amazon.com/cloudwatch/home?region=${region}#dashboards:name=CardArbitrageDashboard`;
    window.open(url, '_blank');
  };

  if (loading && !metrics) {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-7xl mx-auto">
          <div className="animate-pulse">
            <div className="h-8 bg-gray-200 rounded w-64 mb-6"></div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
              {[...Array(4)].map((_, i) => (
                <div key={i} className="h-32 bg-gray-200 rounded-lg"></div>
              ))}
            </div>
            <div className="h-96 bg-gray-200 rounded-lg"></div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 flex items-center">
              <Activity className="w-8 h-8 mr-3 text-blue-600" />
              CloudWatch Dashboard
            </h1>
            <p className="text-gray-600 mt-2">Real-time monitoring of Card Arbitrage infrastructure</p>
          </div>
          <div className="flex items-center space-x-4">
            {lastUpdated && (
              <span className="text-sm text-gray-500">
                Last updated: {lastUpdated.toLocaleTimeString()}
              </span>
            )}
            <button
              onClick={fetchMetrics}
              disabled={loading}
              className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button
              onClick={openCloudWatchConsole}
              className="flex items-center px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              AWS Console
            </button>
          </div>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
            <div className="flex">
              <AlertTriangle className="w-5 h-5 text-red-400 mr-2" />
              <span className="text-red-800">{error}</span>
            </div>
          </div>
        )}

        {/* Overview Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <Zap className="w-8 h-8 text-blue-600" />
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">Total Lambda Invocations</p>
                <p className="text-2xl font-bold text-gray-900">
                  {metrics?.lambdaFunctions.reduce((sum, fn) => sum + fn.invocations, 0) || 0}
                </p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <AlertTriangle className="w-8 h-8 text-red-600" />
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">Total Errors</p>
                <p className="text-2xl font-bold text-gray-900">
                  {metrics?.lambdaFunctions.reduce((sum, fn) => sum + fn.errors, 0) || 0}
                </p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <Database className="w-8 h-8 text-green-600" />
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">DynamoDB Items</p>
                <p className="text-2xl font-bold text-gray-900">
                  {(metrics?.dynamodb.listings_table?.itemCount || 0) + (metrics?.dynamodb.arbitrage_opportunities_table?.itemCount || 0)}
                </p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <TrendingUp className="w-8 h-8 text-purple-600" />
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">API Requests</p>
                <p className="text-2xl font-bold text-gray-900">
                  {metrics?.apiGateway.requests || 0}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Lambda Functions */}
        <div className="bg-white rounded-lg shadow mb-8">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Zap className="w-5 h-5 mr-2 text-blue-600" />
              Lambda Functions
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Function
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Invocations
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Errors
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Success Rate
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Avg Duration
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {metrics?.lambdaFunctions.map((func, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm font-medium text-gray-900">{func.name}</div>
                        <div className="text-sm text-gray-500">{func.fullName}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center">
                          {getStatusIcon(func.status)}
                          <span className="ml-2 text-sm text-gray-900 capitalize">{func.status}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {func.invocations.toLocaleString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        <span className={func.errors > 0 ? 'text-red-600 font-medium' : ''}>
                          {func.errors}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        <span className={getSuccessRate(func.invocations, func.errors) < 95 ? 'text-red-600 font-medium' : 'text-green-600'}>
                          {getSuccessRate(func.invocations, func.errors)}%
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {func.duration.toFixed(1)}ms
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* DynamoDB and Other Services */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
          {/* DynamoDB */}
          <div className="bg-white rounded-lg shadow">
            <div className="px-6 py-4 border-b border-gray-200">
              <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                <Database className="w-5 h-5 mr-2 text-green-600" />
                DynamoDB Tables
              </h2>
            </div>
            <div className="p-6">
              <div className="space-y-6">
                <div>
                  <h3 className="text-sm font-medium text-gray-900 mb-2">Listings Table</h3>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <span className="text-gray-500">Read Capacity:</span>
                      <span className="ml-2 font-medium">{metrics?.dynamodb.listings_table?.readCapacity || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Write Capacity:</span>
                      <span className="ml-2 font-medium">{metrics?.dynamodb.listings_table?.writeCapacity || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Item Count:</span>
                      <span className="ml-2 font-medium">{(metrics?.dynamodb.listings_table?.itemCount || 0).toLocaleString()}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Size:</span>
                      <span className="ml-2 font-medium">{metrics?.dynamodb.listings_table?.size || '0 MB'}</span>
                    </div>
                  </div>
                </div>
                <div>
                  <h3 className="text-sm font-medium text-gray-900 mb-2">Opportunities Table</h3>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <span className="text-gray-500">Read Capacity:</span>
                      <span className="ml-2 font-medium">{metrics?.dynamodb.arbitrage_opportunities_table?.readCapacity || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Write Capacity:</span>
                      <span className="ml-2 font-medium">{metrics?.dynamodb.arbitrage_opportunities_table?.writeCapacity || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Item Count:</span>
                      <span className="ml-2 font-medium">{(metrics?.dynamodb.arbitrage_opportunities_table?.itemCount || 0).toLocaleString()}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Size:</span>
                      <span className="ml-2 font-medium">{metrics?.dynamodb.arbitrage_opportunities_table?.size || '0 MB'}</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* API Gateway & Step Functions */}
          <div className="space-y-8">
            {/* API Gateway */}
            <div className="bg-white rounded-lg shadow">
              <div className="px-6 py-4 border-b border-gray-200">
                <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                  <Server className="w-5 h-5 mr-2 text-purple-600" />
                  API Gateway
                </h2>
              </div>
              <div className="p-6">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-gray-500">Total Requests:</span>
                    <span className="ml-2 font-medium">{metrics?.apiGateway.requests.toLocaleString()}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">4XX Errors:</span>
                    <span className="ml-2 font-medium text-yellow-600">{metrics?.apiGateway.errors4xx}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">5XX Errors:</span>
                    <span className="ml-2 font-medium text-red-600">{metrics?.apiGateway.errors5xx}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Avg Latency:</span>
                    <span className="ml-2 font-medium">{metrics?.apiGateway.latency}ms</span>
                  </div>
                </div>
              </div>
            </div>

            {/* Step Functions */}
            <div className="bg-white rounded-lg shadow">
              <div className="px-6 py-4 border-b border-gray-200">
                <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                  <BarChart3 className="w-5 h-5 mr-2 text-indigo-600" />
                  Step Functions
                </h2>
              </div>
              <div className="p-6">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-gray-500">Total Executions:</span>
                    <span className="ml-2 font-medium">{metrics?.stepFunctions.executions}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Succeeded:</span>
                    <span className="ml-2 font-medium text-green-600">{metrics?.stepFunctions.succeeded}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Failed:</span>
                    <span className="ml-2 font-medium text-red-600">{metrics?.stepFunctions.failed}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Avg Duration:</span>
                    <span className="ml-2 font-medium">{metrics?.stepFunctions.avgDuration}s</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* CloudWatch Integration Status */}
        <div className={`border rounded-lg p-4 ${error ? 'bg-yellow-50 border-yellow-200' : 'bg-green-50 border-green-200'}`}>
          <div className="flex">
            <Activity className={`w-5 h-5 mr-2 ${error ? 'text-yellow-400' : 'text-green-400'}`} />
            <div>
              <h3 className={`text-sm font-medium ${error ? 'text-yellow-800' : 'text-green-800'}`}>
                {error ? 'CloudWatch Integration - Fallback Mode' : 'Live CloudWatch Integration Active'}
              </h3>
              <p className={`text-sm mt-1 ${error ? 'text-yellow-700' : 'text-green-700'}`}>
                {error 
                  ? `API Error: ${error}. Showing mock data as fallback. Check your Lambda function logs for details.`
                  : 'This dashboard is displaying real CloudWatch metrics from your AWS infrastructure via the /metrics API endpoint.'
                }
              </p>
              {error && (
                <p className="text-sm text-yellow-700 mt-2">
                  💡 Ensure your API Gateway is deployed and the Lambda function has CloudWatch permissions.
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CloudWatchDashboard;
