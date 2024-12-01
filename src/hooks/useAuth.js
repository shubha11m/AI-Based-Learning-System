// useAuth.js
import { useState, useEffect } from 'react';
import { login, getCurrentUser, logout } from '../modules/Auth/authService';
import { isAuthenticated } from '../modules/Auth/authUtils';

const useAuth = () => {
  const [user, setUser] = useState(getCurrentUser());
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (isAuthenticated()) {
      setUser(getCurrentUser());
    }
    setLoading(false);
  }, []);

  const loginUser = async (credentials) => {
    try {
      const data = await login(credentials);
      setUser(data.user);
    } catch (error) {
      console.error(error);
    }
  };

  const logoutUser = () => {
    logout();
    setUser(null);
  };

  return { user, loading, loginUser, logoutUser };
};

export default useAuth;
