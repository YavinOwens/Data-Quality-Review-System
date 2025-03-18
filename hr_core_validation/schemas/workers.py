import pandera as pa
from pandera.typing import Series

class WorkersSchema(pa.SchemaModel):
    worker_id: Series[str] = pa.Field(unique=True, nullable=False)
    first_name: Series[str] = pa.Field(nullable=False)
    last_name: Series[str] = pa.Field(nullable=False)
    email: Series[str] = pa.Field(nullable=False, regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    phone: Series[str] = pa.Field(nullable=True, regex=r'^\+?1?\d{9,15}$')
    hire_date: Series[str] = pa.Field(nullable=False, regex=r'^\d{4}-\d{2}-\d{2}$')
    department: Series[str] = pa.Field(nullable=False)
    position: Series[str] = pa.Field(nullable=False)
    salary: Series[float] = pa.Field(nullable=False, ge=0)
    status: Series[str] = pa.Field(nullable=False, isin=['active', 'inactive', 'on_leave'])

workers_schema = WorkersSchema.to_schema() 