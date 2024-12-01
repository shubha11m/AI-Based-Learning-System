// authUtils.js

// Check if user is authenticated by checking token in localStorage
export const isAuthenticated = () => {
    return localStorage.getItem('token') !== null;
  };
  