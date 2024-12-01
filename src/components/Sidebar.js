import React from 'react';
import { Link } from 'react-router-dom';
import './Sidebar.css';

const Sidebar = () => {
  return (
    <aside className="sidebar">
      <ul>
        <li><Link to="/admin/dashboard">Dashboard</Link></li>
        <li><Link to="/admin/users">Users</Link></li>
        <li><Link to="/admin/books">Books</Link></li>
        <li><Link to="/admin/quizzes">Quizzes</Link></li>
        <li><Link to="/admin/reports">Reports</Link></li>
        <li><Link to="/admin/ai-features">AI Features</Link></li>
        <li><Link to="/admin/settings">Settings</Link></li>
      </ul>
    </aside>
  );
};

export default Sidebar;
