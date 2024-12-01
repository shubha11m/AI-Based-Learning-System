import React from 'react';
import StatCard from '../components/StatCard';
import ActivityFeed from '../components/ActivityFeed';
import Graphs from '../components/Graphs';
import './MainContent.css';

const MainContent = () => {
  return (
    <main className="content">
      {/* Quick Stats Section */}
      <div className="overview">
        <StatCard title="Total Users" value={120} icon="👥" />
        <StatCard title="Total Books" value={50} icon="📚" />
        <StatCard title="Total Quizzes" value={30} icon="📝" />
        <StatCard title="AI Interactions" value={300} icon="🤖" />
      </div>

      {/* AI Section */}
      <div className="ai-section">
        {/* Recent AI Activity */}
        <div className="recent-activity">
          <h3>Recent AI Activity</h3>
          <ActivityFeed />
        </div>

        {/* AI Metrics */}
        <div className="ai-metrics">
          <h3>AI Metrics</h3>
          <Graphs />
        </div>

        {/* AI Control Center */}
        <div className="ai-control">
          <h3>AI Control Center</h3>
          <button className="control-btn">Train AI Model</button>
          <button className="control-btn">Monitor AI Performance</button>
        </div>

        {/* System Logs */}
        <div className="system-logs">
          <h3>System Logs</h3>
          <div className="log-item">System updated at 12:00 PM</div>
          <div className="log-item">New AI model version deployed</div>
          <div className="log-item">User John Doe completed a quiz</div>
          <div className="log-item">AI Interaction rate increased by 5%</div>
        </div>
      </div>
    </main>
  );
};

export default MainContent;
