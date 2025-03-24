import streamlit as st

def manage_account_page():
    st.title("Manage Your Account")
    
    # Account Information
    st.header("Account Information")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Profile Details")
        st.text_input("First Name", value="John")
        st.text_input("Last Name", value="Doe")
        st.text_input("Email", value="john.doe@example.com")
        
    with col2:
        st.subheader("Account Settings")
        st.text_input("Username", value="johndoe")
        st.text_input("Current Password", type="password")
        st.text_input("New Password", type="password")
        st.text_input("Confirm New Password", type="password")
    
    # Preferences
    st.header("Preferences")
    st.checkbox("Enable email notifications", value=True)
    st.checkbox("Subscribe to newsletter", value=True)
    
    # Danger Zone
    st.header("Danger Zone")
    if st.button("Delete Account", type="primary"):
        st.warning("Are you sure you want to delete your account? This action cannot be undone.")
        if st.button("Yes, Delete My Account", type="secondary"):
            st.error("Account deletion requested. Please contact support for confirmation.")
    
    # Save Changes
    if st.button("Save Changes", type="primary"):
        st.success("Changes saved successfully!") 