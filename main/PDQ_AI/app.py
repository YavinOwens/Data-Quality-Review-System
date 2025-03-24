import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import json
import psycopg2
from sqlalchemy import create_engine, MetaData, inspect
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from streamlit_option_menu import option_menu
import sys
import os

# Add the project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
main_dir = os.path.dirname(current_dir)  # Get the main directory
project_root = os.path.dirname(main_dir)  # Get the project root

# Add both main directory and project root to Python path
if main_dir not in sys.path:
    sys.path.insert(0, main_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from helpers.db_connection import get_connection, query_to_df
except ImportError as e:
    st.error(f"Failed to import helpers module: {str(e)}")
    st.error(f"Current Python path: {sys.path}")
    st.error(f"Project root: {project_root}")
    st.error(f"Main directory: {main_dir}")
    st.error(f"Current directory: {current_dir}")
    st.stop()

import requests
from typing import List, Dict, Any

# Must be the first Streamlit command
st.set_page_config(layout="wide")

# Add custom CSS
st.markdown("""
<style>
.chat-message {
    padding: 1.5rem;
    border-radius: 0.5rem;
    margin-bottom: 1rem;
    display: flex;
    flex-direction: column;
}
.chat-message.user {
    background-color: #e8f4f8;
}
.chat-message.assistant {
    background-color: #f8f9fa;
}
.chat-message .message-content {
    margin-top: 0.5rem;
}
.chat-message .message-metadata {
    font-size: 0.8rem;
    color: #666;
    margin-top: 0.5rem;
}
</style>
""", unsafe_allow_html=True)

def get_ollama_models() -> List[str]:
    """Get available Ollama models."""
    try:
        response = requests.get("http://localhost:11434/api/tags")
        if response.status_code == 200:
            models = response.json().get("models", [])
            return [model["name"] for model in models]
    except Exception as e:
        st.error(f"Failed to get Ollama models: {str(e)}")
    return []

def query_ollama(model: str, prompt: str) -> str:
    """Query Ollama API with the given model and prompt."""
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False
            }
        )
        if response.status_code == 200:
            return response.json().get("response", "")
    except Exception as e:
        st.error(f"Failed to query Ollama: {str(e)}")
    return ""

def get_database_schema() -> str:
    """Get database schema information."""
    try:
        conn = get_connection()
        tables_query = """
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """
        tables_df = query_to_df(tables_query)
        
        schema_info = []
        for _, row in tables_df.iterrows():
            table_name = row['table_name']
            columns_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position;
            """
            columns_df = query_to_df(columns_query)
            
            columns_info = []
            for _, col in columns_df.iterrows():
                nullable = "NULL" if col['is_nullable'] == "YES" else "NOT NULL"
                columns_info.append(f"{col['column_name']} ({col['data_type']} {nullable})")
            
            schema_info.append(f"Table: {table_name}\nColumns: {', '.join(columns_info)}\n")
        
        return "\n".join(schema_info)
    except Exception as e:
        st.error(f"Failed to get database schema: {str(e)}")
        return ""

def main():
    st.title("Database Q&A Assistant")
    
    # Initialize session state for chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Model selection
    available_models = get_ollama_models()
    if not available_models:
        st.error("No Ollama models found. Please make sure Ollama is running and models are installed.")
        return
    
    selected_model = st.selectbox(
        "Select Ollama Model",
        available_models,
        index=0
    )
    
    # Display chat history
    for message in st.session_state.messages:
        with st.container():
            st.markdown(f"""
            <div class="chat-message {message['role']}">
                <div class="message-content">{message['content']}</div>
                <div class="message-metadata">{message['timestamp']}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # User input
    user_question = st.text_input("Ask a question about the database:", key="user_input")
    
    if user_question:
        # Add user message to chat history
        st.session_state.messages.append({
            "role": "user",
            "content": user_question,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Get database schema
        schema_info = get_database_schema()
        
        # Construct prompt for the model
        prompt = f"""You are a database expert. Here is the database schema:

{schema_info}

User question: {user_question}

Please help answer this question about the database. If the question requires a SQL query, provide the query and explain what it does. If it's a general question about the database structure or relationships, provide a clear explanation.

Response:"""
        
        # Get response from Ollama
        response = query_ollama(selected_model, prompt)
        
        # Add assistant response to chat history
        st.session_state.messages.append({
            "role": "assistant",
            "content": response,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Rerun to display new messages
        st.rerun()

if __name__ == "__main__":
    main()
