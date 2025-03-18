import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import pandera as pa
from typing import Dict, Any
import ydata_profiling
from datetime import datetime
import json
import socket
import psutil
from hr_core_validation.er_diagram import generate_er_diagram

# Add parent directory to path to import HR core validation modules
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import schemas
from hr_core_validation.schemas.workers import workers_schema
from hr_core_validation.schemas.assignments import assignments_schema
from hr_core_validation.schemas.communications import communications_schema
from hr_core_validation.schemas.addresses import addresses_schema

# Define the data sources directory
DATA_SOURCES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'hr_core_validation',
    'data_sources'
)

app = FastAPI(title="HR Core Data Quality API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

def get_schema_for_file(filename: str) -> pa.DataFrameModel:
    """Get the appropriate schema for a given file."""
    schema_map = {
        'workers.csv': workers_schema,
        'assignments.csv': assignments_schema,
        'communications.csv': communications_schema,
        'addresses.csv': addresses_schema
    }
    return schema_map.get(filename)

def validate_file_content(df: pd.DataFrame, schema: pa.DataFrameModel) -> Dict[str, Any]:
    """Validate a DataFrame against a schema."""
    try:
        schema.validate(df)
        return {
            "status": "success",
            "message": "File validation successful",
            "details": {
                "rows": len(df),
                "columns": list(df.columns),
                "validation_passed": True
            }
        }
    except pa.errors.SchemaError as e:
        return {
            "status": "error",
            "message": "Validation failed",
            "details": {
                "rows": len(df),
                "columns": list(df.columns),
                "validation_passed": False,
                "errors": str(e)
            }
        }

@app.get("/")
def read_root():
    return {"message": "Welcome to HR Core Data Quality API"}

@app.post("/validate")
async def validate_file(file: UploadFile = File(...)):
    try:
        # Read the CSV file
        df = pd.read_csv(file.file)
        
        # Determine schema based on filename
        filename = file.filename.lower()
        if 'worker' in filename:
            schema = workers_schema
        elif 'assignment' in filename:
            schema = assignments_schema
        elif 'communication' in filename:
            schema = communications_schema
        elif 'address' in filename:
            schema = addresses_schema
        else:
            raise HTTPException(status_code=400, detail="Unknown file type")
        
        # Validate against schema
        validation_result = validate_file_content(df, schema)
        if validation_result["status"] == "error":
            return JSONResponse(
                status_code=400,
                content=validation_result
            )
        return validation_result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/profile")
async def generate_profile(file: UploadFile = File(...)):
    try:
        # Read the uploaded file
        df = pd.read_csv(file.file)
        
        # Generate profile
        profile = ydata_profiling.ProfileReport(df, title=f"Profile Report for {file.filename}")
        profile_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"profile_{file.filename}.html")
        profile.to_file(profile_path)
        
        return {
            "status": "success",
            "message": "Profile generated successfully",
            "details": {
                "rows": len(df),
                "columns": list(df.columns),
                "profile_url": f"/profile_{file.filename}.html"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/er-diagram")
def get_er_diagram():
    try:
        dot_content = generate_er_diagram()
        return {"dot_content": dot_content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/schema-definitions")
def get_schema_definitions():
    try:
        def schema_to_dict(schema: pa.DataFrameModel) -> dict:
            schema_dict = {}
            for field_name, field in schema.__fields__.items():
                field_info = field.field_info
                field_dict = {
                    "type": str(field.type_),
                    "nullable": field_info.nullable,
                    "required": True
                }
                if hasattr(field_info, "regex"):
                    field_dict["regex"] = field_info.regex
                if hasattr(field_info, "isin"):
                    field_dict["allowed_values"] = field_info.isin
                if hasattr(field_info, "unique"):
                    field_dict["unique"] = field_info.unique
                if hasattr(field_info, "ge"):
                    field_dict["min_value"] = field_info.ge
                if hasattr(field_info, "le"):
                    field_dict["max_value"] = field_info.le
                schema_dict[field_name] = field_dict
            return schema_dict

        schemas = {
            "Workers": schema_to_dict(workers_schema),
            "Assignments": schema_to_dict(assignments_schema),
            "Communications": schema_to_dict(communications_schema),
            "Addresses": schema_to_dict(addresses_schema)
        }
        return schemas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def cleanup_port(port):
    try:
        for proc in psutil.process_iter(['pid', 'name', 'connections']):
            try:
                for conn in proc.connections():
                    if conn.laddr.port == port:
                        psutil.Process(proc.pid).terminate()
                        print(f"Terminated process {proc.pid} using port {port}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception as e:
        print(f"Error cleaning up port: {e}")

if __name__ == "__main__":
    import uvicorn
    port = 5001
    cleanup_port(port)
    uvicorn.run(app, host="0.0.0.0", port=port) 