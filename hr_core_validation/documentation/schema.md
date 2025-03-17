# HR Core Data Schema Documentation

## Workers Table
Primary key: PERSON_ID, UNIQUE_ID

| Column | Type | Description |
|--------|------|-------------|
| PERSON_ID | Integer | Primary identifier |
| UNIQUE_ID | UUID | Unique identifier for record linking |
| EMPLOYEE_NUMBER | String | Employee number |
| FIRST_NAME | String | First name |
| LAST_NAME | String | Last name |
| SEX | String | Gender (M/F/X) |
| DATE_OF_BIRTH | Date | Date of birth |
| NATIONAL_IDENTIFIER | String | National ID/SSN |
| CURRENT_EMPLOYEE_FLAG | String | Current employee indicator |
| EFFECTIVE_START_DATE | Date | Record start date |
| EFFECTIVE_END_DATE | Date | Record end date |

## Assignments Table
Primary key: ASSIGNMENT_ID, UNIQUE_ID
Foreign key: PERSON_ID, WORKER_UNIQUE_ID -> Workers(PERSON_ID, UNIQUE_ID)

| Column | Type | Description |
|--------|------|-------------|
| ASSIGNMENT_ID | Integer | Primary identifier |
| UNIQUE_ID | UUID | Unique identifier for record linking |
| PERSON_ID | Integer | Foreign key to Workers |
| WORKER_UNIQUE_ID | UUID | Foreign key to Workers |
| EFFECTIVE_START_DATE | Date | Record start date |
| EFFECTIVE_END_DATE | Date | Record end date |
| ASSIGNMENT_TYPE | String | Type of assignment |
| ASSIGNMENT_STATUS_TYPE_ID | Integer | Status identifier |
| BUSINESS_GROUP_ID | Integer | Business group identifier |
| ORGANIZATION_ID | Integer | Organization identifier |
| JOB_ID | Integer | Job identifier |
| POSITION_ID | Integer | Position identifier |
| GRADE_ID | Integer | Grade identifier |

## Addresses Table
Primary key: ADDRESS_ID, UNIQUE_ID
Foreign key: PERSON_ID, WORKER_UNIQUE_ID -> Workers(PERSON_ID, UNIQUE_ID)

| Column | Type | Description |
|--------|------|-------------|
| ADDRESS_ID | Integer | Primary identifier |
| UNIQUE_ID | UUID | Unique identifier for record linking |
| PERSON_ID | Integer | Foreign key to Workers |
| WORKER_UNIQUE_ID | UUID | Foreign key to Workers |
| EFFECTIVE_START_DATE | Date | Record start date |
| EFFECTIVE_END_DATE | Date | Record end date |
| ADDRESS_TYPE | String | Type of address |
| ADDRESS_LINE1 | String | Street address |
| CITY | String | City name |
| POSTAL_CODE | String | Postal/ZIP code |
| COUNTRY | String | Country code |

## Communications Table
Primary key: CONTACT_ID, UNIQUE_ID
Foreign key: PERSON_ID, WORKER_UNIQUE_ID -> Workers(PERSON_ID, UNIQUE_ID)

| Column | Type | Description |
|--------|------|-------------|
| CONTACT_ID | Integer | Primary identifier |
| UNIQUE_ID | UUID | Unique identifier for record linking |
| PERSON_ID | Integer | Foreign key to Workers |
| WORKER_UNIQUE_ID | UUID | Foreign key to Workers |
| EFFECTIVE_START_DATE | Date | Record start date |
| EFFECTIVE_END_DATE | Date | Record end date |
| CONTACT_TYPE | String | Type of contact |
| CONTACT_VALUE | String | Contact information |
| PRIMARY_FLAG | String | Primary contact indicator |
