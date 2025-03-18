import pandera as pa
from pandera.typing import Series

class AddressesSchema(pa.SchemaModel):
    address_id: Series[str] = pa.Field(unique=True, nullable=False)
    worker_id: Series[str] = pa.Field(nullable=False)
    street_address: Series[str] = pa.Field(nullable=False)
    city: Series[str] = pa.Field(nullable=False)
    state: Series[str] = pa.Field(nullable=False)
    postal_code: Series[str] = pa.Field(nullable=False, regex=r'^\d{5}(-\d{4})?$')
    country: Series[str] = pa.Field(nullable=False)
    address_type: Series[str] = pa.Field(nullable=False, isin=['home', 'work', 'mailing'])
    is_primary: Series[bool] = pa.Field(nullable=False)

addresses_schema = AddressesSchema.to_schema() 