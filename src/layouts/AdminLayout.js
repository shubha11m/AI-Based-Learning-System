import React from 'react';
import Sidebar from '../components/Sidebar';
import Header from '../components/Header';
import MainContent from '../components/MainContent';
import './AdminLayout.css';
import Footer from '../components/Footer';

const AdminLayout = () => {
  return (
    <div className="admin-layout">
      <Sidebar />
      <div className="main-area">
        <Header />
        <div className="content-area">
          {/* Main Content goes here */}
          <MainContent />
        </div>
        <Footer />
      </div>
    </div>
  );
};


export default AdminLayout;
