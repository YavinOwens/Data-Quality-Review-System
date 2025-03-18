# HR Core Data Quality Analyzer

A web application for analyzing and validating HR core data files. Built with FastAPI, React, and Material-UI.

## Features

- Data Validation: Validate CSV files against predefined schemas
- Data Profiling: Generate detailed data quality reports
- Schema Visualization: View and explore data schemas
- ER Diagram Generation: Visualize relationships between data entities

## Tech Stack

### Backend
- FastAPI
- Pandas
- Pandera
- YData Profiling
- SQLAlchemy

### Frontend
- React
- Material-UI
- Vite
- TypeScript

## Getting Started

1. Clone the repository
```bash
git clone <repository-url>
cd ollama-analyzer
```

2. Install dependencies and start the application
```bash
./start.sh
```

The application will be available at:
- Frontend: http://localhost:5173
- Backend: http://localhost:5001

## Project Structure

```
ollama-analyzer/
├── backend/
│   ├── app.py                 # FastAPI application
│   ├── requirements.txt       # Python dependencies
│   └── hr_core_validation/   # Core validation modules
├── src/
│   ├── components/           # React components
│   ├── api/                  # API client
│   └── App.tsx              # Main application component
├── package.json             # Node.js dependencies
└── start.sh                # Application startup script
```

## Data Schemas

The application supports validation for the following HR data types:
- Workers
- Assignments
- Communications
- Addresses

Each schema defines required fields, data types, and validation rules.

## Development

To start the application in development mode:

1. Start the backend server:
```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --port 5001
```

2. Start the frontend development server:
```bash
npm install
npm run dev
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License. 