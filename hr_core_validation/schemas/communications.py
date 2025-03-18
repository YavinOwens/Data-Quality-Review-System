import pandera as pa
from pandera.typing import Series

class CommunicationsSchema(pa.SchemaModel):
    communication_id: Series[str] = pa.Field(unique=True, nullable=False)
    worker_id: Series[str] = pa.Field(nullable=False)
    type: Series[str] = pa.Field(nullable=False, isin=['email', 'phone', 'meeting', 'letter'])
    date: Series[str] = pa.Field(nullable=False, regex=r'^\d{4}-\d{2}-\d{2}$')
    subject: Series[str] = pa.Field(nullable=False)
    content: Series[str] = pa.Field(nullable=False)
    status: Series[str] = pa.Field(nullable=False, isin=['sent', 'received', 'draft', 'archived'])
    priority: Series[str] = pa.Field(nullable=False, isin=['low', 'medium', 'high'])

communications_schema = CommunicationsSchema.to_schema() 