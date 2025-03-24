import streamlit as st

def learn_page():
    st.title("Learn About Us")
    
    # Introduction
    st.header("Welcome to EBS Data Explorer")
    st.write("""
    EBS Data Explorer is a powerful tool designed to help you explore and analyze your EBS (Enterprise Business System) data.
    Our platform provides intuitive interfaces and powerful features to make data exploration easy and efficient.
    """)
    
    # Features
    st.header("Key Features")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Data Visualization")
        st.write("""
        - Interactive charts and graphs
        - Customizable dashboards
        - Real-time data updates
        """)
    
    with col2:
        st.subheader("Data Analysis")
        st.write("""
        - Advanced filtering options
        - Data export capabilities
        - Custom queries
        """)
    
    with col3:
        st.subheader("Security")
        st.write("""
        - Role-based access control
        - Data encryption
        - Audit logging
        """)
    
    # Getting Started
    st.header("Getting Started")
    st.write("""
    1. Create your account
    2. Set up your preferences
    3. Start exploring your data
    """)
    
    # FAQ
    st.header("Frequently Asked Questions")
    with st.expander("What is EBS Data Explorer?"):
        st.write("EBS Data Explorer is a comprehensive tool for exploring and analyzing your enterprise business system data.")
    
    with st.expander("How do I get started?"):
        st.write("Simply create an account and follow our getting started guide to begin exploring your data.")
    
    with st.expander("Is my data secure?"):
        st.write("Yes, we take security seriously. All data is encrypted and protected with industry-standard security measures.") 