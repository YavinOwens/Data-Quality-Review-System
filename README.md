# SQL Analysis Assistant with Phi-4 Mini

A Streamlit application that provides an interactive interface for Oracle database analysis using the Phi-4 Mini language model.

## Prerequisites

- Docker
- Python 3.8+
- Oracle Database running in Docker
- Ollama running in Docker

## Setup

1. Pull the Phi-4 Mini model:
```bash
ollama pull phi4-mini
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
- Copy `.env.example` to `.env`
- Update the Oracle connection details in `.env`

4. Create a `secrets.toml` file in `.streamlit/` directory:
```toml
[oracle]
user = "your_username"
password = "your_password"
dsn = "localhost:1521/your_service_name"
```

## Running the Application

1. Start Ollama (if not already running):
```bash
docker start ollama
```

2. Run the Streamlit app:
```bash
streamlit run app.py
```

## Features

- Interactive SQL query editor with real-time analysis
- Schema explorer with table statistics
- Query history and performance metrics
- AI-powered query analysis and optimization suggestions
- Natural language query explanations

## Architecture

The application consists of three main components:
1. Streamlit frontend for user interaction
2. Oracle database for data storage and querying
3. Phi-4 Mini model for AI-powered analysis

## Security Notes

- Never commit sensitive credentials to version control
- Use environment variables for configuration
- Follow Oracle security best practices
- Keep Ollama and dependencies updated
