import React, { useState } from 'react';
import { MDBContainer, MDBCol, MDBRow, MDBBtn, MDBInput, MDBCheckbox, MDBIcon } from 'mdb-react-ui-kit';
import { useNavigate } from 'react-router-dom';
import { login } from '../services/authService'; // Importing login service

function LoginPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();  // React Router hook for navigation

  // Handle login form submission
  const handleLogin = async () => {
    try {
      const response = await login({ email, password });
      // If login is successful, redirect to dashboard based on role
      if (response.role === 'ADMIN') {
        navigate('/dashboard/admin');
      } else if (response.role === 'TEACHER') {
        navigate('/dashboard/teacher');
      } else if (response.role === 'STUDENT') {
        navigate('/dashboard/student');
      }
    } catch (err) {
      setError('Invalid credentials. Please try again.');
    }
  };

  return (
    <MDBContainer fluid className="p-3 my-5">
      <MDBRow>
        <MDBCol col='10' md='6'>
          <img src="https://mdbcdn.b-cdn.net/img/Photos/new-templates/bootstrap-login-form/draw2.svg" className="img-fluid" alt="Phone image" />
        </MDBCol>

        <MDBCol col='4' md='6'>
          {error && <div className="alert alert-danger">{error}</div>}
          <MDBInput wrapperClass='mb-4' label='Email address' id='formControlLg' type='email' size="lg" value={email} onChange={(e) => setEmail(e.target.value)} />
          <MDBInput wrapperClass='mb-4' label='Password' id='formControlLg' type='password' size="lg" value={password} onChange={(e) => setPassword(e.target.value)} />

          <div className="d-flex justify-content-between mx-4 mb-4">
            <MDBCheckbox name='flexCheck' id='flexCheckDefault' label='Remember me' />
            <a href="#!">Forgot password?</a>
          </div>

          <MDBBtn className="mb-4 w-100" size="lg" onClick={handleLogin}>Sign in</MDBBtn>

          <div className="divider d-flex align-items-center my-4">
            <p className="text-center fw-bold mx-3 mb-0">OR</p>
          </div>

          <MDBBtn className="mb-4 w-100" size="lg" style={{ backgroundColor: '#3b5998' }}>
            <MDBIcon fab icon="facebook-f" className="mx-2" />
            Continue with Facebook
          </MDBBtn>

          <MDBBtn className="mb-4 w-100" size="lg" style={{ backgroundColor: '#55acee' }}>
            <MDBIcon fab icon="twitter" className="mx-2" />
            Continue with Twitter
          </MDBBtn>
        </MDBCol>
      </MDBRow>
    </MDBContainer>
  );
}

export default LoginPage;
