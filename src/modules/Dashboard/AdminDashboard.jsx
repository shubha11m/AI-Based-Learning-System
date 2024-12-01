import React, { useState, useEffect } from 'react';
import StatCard from '../../components/StatCard'; // Custom component for stat display
import Graphs from '../../components/Graphs'; // Custom component for graphs
import ActivityFeed from '../../components/ActivityFeed'; // Custom component for activity feed
import './AdminDashboard.css'; // AdminDashboard styling
import useAuth  from '../../hooks/useAuth'; // Hook to access auth info

const AdminDashboard = () => {
  const { role } = useAuth();  // Get user role from auth context
  const [dashboardData, setDashboardData] = useState({
    totalUsers: 120,
    totalBooks: 50,
    totalQuizzes: 30,
    aiInteractions: 300,
  });

  const [activityFeed, setActivityFeed] = useState([
    { id: 1, message: 'New user registered' },
    { id: 2, message: 'AI model trained successfully' },
    { id: 3, message: 'Quiz #12 created' },
  ]);

  useEffect(() => {
    // Fetch dynamic data for dashboard (e.g., via an API call)
    // For now, we use static data, but this can be updated dynamically
    if (role === 'admin') {
      // Fetch admin-specific data if necessary
    }
  }, [role]);

  return (
    <div className="admin-dashboard">
      <h2>Admin Dashboard</h2>

      {/* Overview Section */}
      <div className="overview">
        <StatCard title="Total Users" value={dashboardData.totalUsers} />
        <StatCard title="Total Books" value={dashboardData.totalBooks} />
        <StatCard title="Total Quizzes" value={dashboardData.totalQuizzes} />
        <StatCard title="AI Interactions" value={dashboardData.aiInteractions} />
      </div>

      {/* Activity Feed */}
      <div className="activity-feed">
        <h3>Recent Activity</h3>
        <ActivityFeed activities={activityFeed} />
      </div>

      {/* Graphs Section */}
      <div className="graphs-section">
        <h3>System Performance & AI Metrics</h3>
        <Graphs />
      </div>

      {/* AI Control and Logs */}
      <div className="logs-ai-control">
        <div className="system-logs">
          <h3>System Logs</h3>
          <div className="log-item">System updated at 12:00 PM</div>
          <div className="log-item">New AI model version deployed</div>
        </div>

        <div className="ai-control">
          <h3>AI Control</h3>
          <button onClick={() => alert("Train AI Model")}>Train AI Model</button>
          <button onClick={() => alert("Monitor AI Performance")}>Monitor AI Performance</button>
        </div>
      </div>
    </div>
  );
};

export default AdminDashboard;
