import streamlit as st

def create_account_page():
    st.title("Create Your Account")
    
    with st.form("create_account_form"):
        st.subheader("Personal Information")
        first_name = st.text_input("First Name")
        last_name = st.text_input("Last Name")
        email = st.text_input("Email")
        
        st.subheader("Account Details")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        
        st.subheader("Preferences")
        notifications = st.checkbox("Enable email notifications")
        newsletter = st.checkbox("Subscribe to newsletter")
        
        submitted = st.form_submit_button("Create Account")
        
        if submitted:
            if password != confirm_password:
                st.error("Passwords do not match!")
            else:
                # Here you would typically handle the account creation
                st.success("Account created successfully!")
                st.balloons() 