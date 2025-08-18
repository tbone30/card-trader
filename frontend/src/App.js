import React, { useState } from 'react';
import CardArbitrageDashboard from './CardArbitrageDashboard';
import CloudWatchDashboard from './CloudWatchDashboard';
import { Activity, TrendingUp } from 'lucide-react';

function App() {
  const [currentPage, setCurrentPage] = useState('arbitrage');

  const navigation = [
    { id: 'arbitrage', name: 'Arbitrage Dashboard', icon: TrendingUp },
    { id: 'monitoring', name: 'CloudWatch Monitoring', icon: Activity }
  ];

  const renderPage = () => {
    switch (currentPage) {
      case 'arbitrage':
        return <CardArbitrageDashboard />;
      case 'monitoring':
        return <CloudWatchDashboard />;
      default:
        return <CardArbitrageDashboard />;
    }
  };

  return (
    <div className="App min-h-screen bg-gray-50">
      {/* Navigation Header */}
      <nav className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex justify-between h-16">
            <div className="flex">
              <div className="flex-shrink-0 flex items-center">
                <h1 className="text-xl font-bold text-gray-900">Card Arbitrage</h1>
              </div>
              <div className="hidden sm:ml-6 sm:flex sm:space-x-8">
                {navigation.map((item) => {
                  const Icon = item.icon;
                  return (
                    <button
                      key={item.id}
                      onClick={() => setCurrentPage(item.id)}
                      className={`${
                        currentPage === item.id
                          ? 'border-blue-500 text-blue-600'
                          : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                      } whitespace-nowrap py-2 px-1 border-b-2 font-medium text-sm flex items-center`}
                    >
                      <Icon className="w-4 h-4 mr-2" />
                      {item.name}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </nav>

      {/* Mobile Navigation */}
      <div className="sm:hidden bg-white border-b border-gray-200">
        <div className="px-2 pt-2 pb-3 space-y-1">
          {navigation.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                onClick={() => setCurrentPage(item.id)}
                className={`${
                  currentPage === item.id
                    ? 'bg-blue-50 border-blue-500 text-blue-700'
                    : 'border-transparent text-gray-600 hover:bg-gray-50 hover:text-gray-800'
                } block pl-3 pr-4 py-2 border-l-4 text-base font-medium w-full text-left flex items-center`}
              >
                <Icon className="w-4 h-4 mr-2" />
                {item.name}
              </button>
            );
          })}
        </div>
      </div>

      {/* Page Content */}
      {renderPage()}
    </div>
  );
}

export default App;