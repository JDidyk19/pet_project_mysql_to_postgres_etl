from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr


class Person(BaseModel):
    
    # identity
    person_id: UUID
    first_name: str
    last_name: str
    middle_name: Optional[str] = None
    gender: str
    birth_date: date
    age: int
    
    # contacts
    email: EmailStr
    phone_number: str
    
    # address
    country: str
    city: str
    
    # professional
    company_name: str
    salary_usd: Decimal
    is_employed: bool
    
    # timestamps
    created_at: datetime
    updated_at: datetime
    
    class Config:
        json_encoders = {
            date: lambda date_: date_.isoformat(),
            datetime: lambda datetime_: datetime_.isoformat(),
        }
        