import os
from datetime import date
from pathlib import Path
from uuid import uuid4
import logging

import pandas as pd
from faker import Faker
from models.person import Person
from config.logger import setup_logger

FAKE_DATA_DIR_PATH = Path('.') / 'data'
FAKE_DATA_FILE_PATH = FAKE_DATA_DIR_PATH / 'fake_data.csv'

setup_logger()
logger = logging.getLogger(__name__)


class FakeDataGenerator:
    """Generate person dataset."""
    
    def __init__(self):
        self.faker = Faker()
    
    def generate_fake_data(self, size: int = 1000) -> list[Person]:
        """Generate fake people records

        Args:
            size (int)

        Returns:
            list[Person]
        """
        
        logger.info(f"Starting generation of {size} records")
        people: list[Person] = []
        for _ in range(size):
            birth_date = self.faker.date_of_birth(minimum_age=18, maximum_age=65)
            today = date.today()
            age = today.year - birth_date.year - (
                (today.month, today.day) < (birth_date.month, birth_date.day)
            )
            
            people.append(
                Person(
                    person_id=uuid4(),
                    first_name=self.faker.first_name(),
                    last_name=self.faker.last_name(),
                    gender=self.faker.passport_gender(),
                    birth_date=birth_date,
                    age=age,
                    email=self.faker.email(),
                    phone_number=self.faker.phone_number(),
                    country=self.faker.country(),
                    city=self.faker.city(),
                    company_name=self.faker.company(),
                    salary_usd=self.faker.pydecimal(
                        left_digits=6,
                        right_digits=2,
                        positive=True,
                        min_value=50000,
                        max_value=150000,
                    ),
                    is_employed=self.faker.boolean(),
                    created_at=self.faker.date_time_between(
                        start_date="-10y", end_date="-1y"
                    ),
                    updated_at=self.faker.date_time_between(
                        start_date="-1y", end_date="now"
                    ),
                )
            )
            
        logger.info(f"Successfully generated {len(people)} records")
        return people
    
    def to_csv(self, people: list[Person], output_path: str = FAKE_DATA_FILE_PATH) -> None:
        """Save generated records to CSV.

        Args:
            people: List of Person records.
            output_path: Destination CSV path.
        """
        logger.info(f"Saving records to CSV: {output_path}")
        
        if not FAKE_DATA_DIR_PATH.exists():
            os.mkdir(FAKE_DATA_DIR_PATH)
            
        if not people:
            raise ValueError('"people" is empty, you have might not generated data yet, ' \
                            'please call "generate_fake_data".')

        df = pd.DataFrame([person.model_dump() for person in people])
        df.to_csv(output_path, index=False)
        
        logger.info(f"CSV successfully saved with {len(df)} rows")


if __name__ == '__main__':
    DATA_SIZE = int(os.getenv('DATA_SIZE', '1000'))
    FAKE_DATA_FILE_NAME = os.getenv('FAKE_DATA_FILE_NAME', 'fake_data') + '.csv'
    file_path = FAKE_DATA_DIR_PATH / FAKE_DATA_FILE_NAME
    
    generator = FakeDataGenerator()
    people = generator.generate_fake_data(size=DATA_SIZE)
    generator.to_csv(people, file_path)
