import React from 'react';
import './Header.css';

const Header = () => {
  return (
    <header className="header">
      <div className="header-left">
        <span className="page-title">Admin Dashboard</span>
      </div>
      <div className="header-right">
        <input
          type="text"
          className="search-box"
          placeholder="Search..."
        />
        <div className="header-icons">
          <span className="icon" title="Notifications">
            🔔 <span className="badge">3</span>
          </span>
          <span className="icon" title="Settings">
            ⚙️
          </span>
          <div className="user-profile">
            <img
              src="/path/to/avatar.jpg"
              alt="User Avatar"
              className="avatar"
            />
            <span className="username">John Doe</span>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;
