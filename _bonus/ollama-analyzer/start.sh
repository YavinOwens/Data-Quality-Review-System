#!/bin/bash

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to cleanup ports
cleanup_ports() {
    echo "Cleaning up ports..."
    # Kill processes on frontend ports (5173-5182)
    for port in $(seq 5173 5182); do
        lsof -ti :$port | xargs kill -9 2>/dev/null
    done
    # Kill process on backend port (5001)
    lsof -ti :5001 | xargs kill -9 2>/dev/null
}

# Check for required commands
if ! command_exists python3; then
    echo "Python 3 is required but not installed. Please install Python 3 and try again."
    exit 1
fi

if ! command_exists node; then
    echo "Node.js is required but not installed. Please install Node.js and try again."
    exit 1
fi

if ! command_exists npm; then
    echo "npm is required but not installed. Please install npm and try again."
    exit 1
fi

# Clean up any existing processes
cleanup_ports

# Create and activate Python virtual environment if it doesn't exist
if [ ! -d "backend/venv" ]; then
    echo "Creating Python virtual environment..."
    cd backend
    python3 -m venv venv
    cd ..
fi

# Activate virtual environment and install backend dependencies
echo "Installing backend dependencies..."
source backend/venv/bin/activate
cd backend
pip install -r requirements.txt
cd ..

# Install frontend dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "Installing frontend dependencies..."
    npm install
fi

# Start backend server
echo "Starting backend server..."
cd backend
python app.py &
cd ..

# Wait for backend to start
sleep 2

# Start frontend development server
echo "Starting frontend server..."
npm run dev &

# Wait for both processes
echo "Services are starting..."
echo "Frontend will be available at http://localhost:5173 (or next available port)"
echo "Backend is running at http://localhost:5001"
echo "Press Ctrl+C to stop all services"

# Handle cleanup on script termination
trap 'cleanup_ports; exit 0' SIGINT SIGTERM

# Keep script running
wait 