import React from 'react';
import Header from '../components/Header';
import Sidebar from '../components/Sidebar';
import Footer from '../components/Footer';

const StudentLayout = ({ children }) => (
  <div style={{ display: 'flex' }}>
    <Sidebar role="student" />
    <div style={{ flex: 1 }}>
      <Header />
      <main>{children}</main>
      <Footer />
    </div>
  </div>
);

export default StudentLayout ;
