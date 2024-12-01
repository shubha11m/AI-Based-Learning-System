import React from 'react';
import { Route, Routes } from 'react-router-dom';
import LoginPage from '../modules/Auth/loginPage';
import AdminLayout from '../layouts/AdminLayout'; // Make sure this is a default export
import TeacherLayout from '../layouts/TeacherLayout'; // Same here
import StudentLayout from '../layouts/StudentLayout';
import AdminDashboard from '../modules/Dashboard/AdminDashboard'; // Ensure this is a default export or named correctly
import TeacherDashboard from '../modules/Dashboard/TeacherDashboard';
import StudentDashboard from '../modules/Dashboard/StudentDashboard';
import PrivateRoute from '../routes/PrivateRoute';

const AppRoutes = () => {
  return (
    <Routes>
      <Route path="/" element={<LoginPage />} />
      
      {/* Protected routes */}
      <Route path="*" element={<h1>404 Not Found</h1>} />
      <Route 
        path="/admin/dashboard" 
        element={
          <PrivateRoute role="admin">
            <AdminLayout>
              <AdminDashboard />
            </AdminLayout>
          </PrivateRoute>
        } 
      />
      
      <Route 
        path="/teacher/dashboard" 
        element={
          <PrivateRoute role="teacher">
            <TeacherLayout>
              <TeacherDashboard />
            </TeacherLayout>
          </PrivateRoute>
        } 
      />
      
      <Route 
        path="/student/dashboard" 
        element={
          <PrivateRoute role="student">
            <StudentLayout>
              <StudentDashboard />
            </StudentLayout>
          </PrivateRoute>
        } 
      />
    </Routes>
  );
};

export default AppRoutes;
