import React from 'react';
import Header from '../components/Header';
import Sidebar from '../components/Sidebar';
import Footer from '../components/Footer';

const TeacherLayout = ({ children }) => (
  <div style={{ display: 'flex' }}>
    <Sidebar role="admin" />
    <div style={{ flex: 1 }}>
      <Header />
      <main>{children}</main>
      <Footer />
    </div>
  </div>
);

export default TeacherLayout;
