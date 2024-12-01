// authService.js

// Function to handle user login
export const login = async (credentials) => {
  try {
    // Dummy data for testing (or replace with your API endpoint)
    const dummyUsers = [
      { email: 'admin@school.com', password: 'admin123', role: 'admin' },
      { email: 'teacher@school.com', password: 'teacher123', role: 'TEACHER' },
      { email: 'student@school.com', password: 'student123', role: 'STUDENT' },
    ];

    // Log the credentials entered by the user
    console.log('Attempting to log in with credentials:', credentials);

    // Find the user that matches the entered email and password
    const user = dummyUsers.find(
      (user) => user.email === credentials.email && user.password === credentials.password
    );

    if (user) {
      // Log the matched user data
      console.log('User found:', user);

      // Mock response object from API
      const data = {
        token: 'mock-token-123',
        user: user,
      };

      // Save token and user data to localStorage
      localStorage.setItem('token', data.token);
      localStorage.setItem('user', JSON.stringify(data.user));

      return data;
    } else {
      // Log invalid credentials
      console.log('Invalid credentials entered');
      throw new Error('Invalid credentials');
    }
  } catch (error) {
    // Log the error if something goes wrong
    console.error('Error during login:', error);
    throw error;
  }
};

// Function to get the current user
export const getCurrentUser = () => {
  const user = localStorage.getItem('user');
  return user ? JSON.parse(user) : null;
};

// Function to get the authentication token
export const getToken = () => {
  return localStorage.getItem('token');
};

// Function to log out the user
export const logout = () => {
  localStorage.removeItem('token');
  localStorage.removeItem('user');
};
