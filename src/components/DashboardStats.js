import React from 'react';

const DashboardStats = () => {
  // Example statistics (these can later be fetched from your API)
  const stats = [
    { label: 'Total Users', value: 1200, color: '#3498db' },
    { label: 'Active Books', value: 150, color: '#e74c3c' },
    { label: 'Quizzes Taken', value: 450, color: '#2ecc71' },
    { label: 'AI Usage', value: 125, color: '#f39c12' }
  ];

  return (
    <div style={statsContainerStyle}>
      {stats.map((stat, index) => (
        <div key={index} style={{ ...cardStyle, backgroundColor: stat.color }}>
          <h3 style={statValueStyle}>{stat.value}</h3>
          <p style={statLabelStyle}>{stat.label}</p>
        </div>
      ))}
    </div>
  );
};

const statsContainerStyle = {
  display: 'grid',
  gridTemplateColumns: 'repeat(4, 1fr)',
  gap: '1rem',
  marginBottom: '2rem'
};

const cardStyle = {
  padding: '1rem',
  borderRadius: '8px',
  color: 'white',
  textAlign: 'center',
  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
};

const statValueStyle = {
  fontSize: '2rem',
  fontWeight: 'bold',
  margin: '0'
};

const statLabelStyle = {
  fontSize: '1rem',
  marginTop: '0.5rem'
};

export default DashboardStats;
