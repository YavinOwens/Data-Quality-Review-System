import pandera as pa
from pandera.typing import Series

class AssignmentsSchema(pa.SchemaModel):
    assignment_id: Series[str] = pa.Field(unique=True, nullable=False)
    worker_id: Series[str] = pa.Field(nullable=False)
    project_name: Series[str] = pa.Field(nullable=False)
    start_date: Series[str] = pa.Field(nullable=False, regex=r'^\d{4}-\d{2}-\d{2}$')
    end_date: Series[str] = pa.Field(nullable=True, regex=r'^\d{4}-\d{2}-\d{2}$')
    role: Series[str] = pa.Field(nullable=False)
    allocation_percentage: Series[float] = pa.Field(nullable=False, ge=0, le=100)
    status: Series[str] = pa.Field(nullable=False, isin=['active', 'completed', 'on_hold'])

assignments_schema = AssignmentsSchema.to_schema() 