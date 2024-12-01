import React, { useState } from 'react';
import { MDBContainer, MDBCol, MDBRow, MDBBtn, MDBIcon, MDBInput, MDBCheckbox } from 'mdb-react-ui-kit';
import { useNavigate } from 'react-router-dom';
import { login } from './authService';
import 'mdb-react-ui-kit/dist/css/mdb.min.css';

function LoginPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  // Inline style for the body
  const containerStyle = {
    backgroundColor: "#fbfbfb",  // Background color for the page
    height: "100vh",  // Full viewport height
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    padding: "0", // No padding
  };

  const formStyle = {
    backgroundColor: "#fff",  // Background color for the form
    padding: "30px",  // Padding for the form
    borderRadius: "10px",  // Rounded corners
    boxShadow: "0px 4px 6px rgba(0, 0, 0, 0.1)",  // Shadow for form
    width: "100%",  // Ensure form takes full width
    maxWidth: "500px",  // Max width for larger screens
  };

  const dividerStyle = {
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    margin: "20px 0",
  };

  const orTextStyle = {
    margin: "0 20px",
    fontWeight: "bold",
  };

  const handleLogin = async () => {
    try {
      const response = await login({ email, password });
      console.log("res", response.user.role);  // Check if the role is correct
      if (response.user.role === 'admin') {
        navigate('/admin/dashboard');
      } else if (response.user.role === 'TEACHER') {
        navigate('/teacher/dashboard');
      } else if (response.user.role === 'STUDENT') {
        navigate('/student/dashboard');
      }
    } catch (err) {
      setError('Invalid credentials. Please try again.');
    }
  };
  

  return (
    <div style={containerStyle}>
      <MDBContainer fluid className="p-0" style={{ width: "100%" }}>
        <MDBRow style={{ height: "100%", width: "100%" }}>
          {/* Image column with margin-right */}
          <MDBCol col='12' md='6' style={{ display: "flex", justifyContent: "center", alignItems: "center" }}>
            <img
              src="https://mdbcdn.b-cdn.net/img/Photos/new-templates/bootstrap-login-form/draw2.svg"
              alt="Illustration of a phone with a message"
              style={{ width: "80%", height: "auto", objectFit: "contain" }} // Fit image without stretching
            />
          </MDBCol>

          {/* Form column */}
          <MDBCol col='12' md='6' className="d-flex justify-content-center align-items-center">
            <div style={formStyle}>
              {error && <div style={{ color: "red" }}>{error}</div>}
              <MDBInput
                wrapperClass='mb-4'
                label='Email address'
                id='formControlLg'
                type='email'
                size="lg"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
              <MDBInput
                wrapperClass='mb-4'
                label='Password'
                id='formControlLg'
                type='password'
                size="lg"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />

              <div className="d-flex justify-content-between mx-4 mb-4">
                <MDBCheckbox name='flexCheck' value='' id='flexCheckDefault' label='Remember me' />
                <a href="#!">Forgot password?</a>
              </div>

              <MDBBtn className="mb-4 w-100" size="lg" onClick={handleLogin}>
                Sign in
              </MDBBtn>

              {/* Divider with "OR" text */}
              <div style={dividerStyle}>
                <p style={orTextStyle}>OR</p>
              </div>

              {/* Social Media Login Buttons */}
              <MDBBtn className="mb-4 w-100" size="lg" style={{ backgroundColor: '#3b5998' }}>
                <MDBIcon fab icon="facebook-f" className="mx-2" />
                Continue with Facebook
              </MDBBtn>

              <MDBBtn className="mb-4 w-100" size="lg" style={{ backgroundColor: '#55acee' }}>
                <MDBIcon fab icon="twitter" className="mx-2" />
                Continue with Twitter
              </MDBBtn>
            </div>
          </MDBCol>
        </MDBRow>
      </MDBContainer>
    </div>
  );
}

export default LoginPage;
