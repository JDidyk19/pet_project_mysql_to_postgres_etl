USE source_db;

CREATE TABLE IF NOT EXISTS users (
    person_id CHAR(36) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(100) NULL,
    gender CHAR(1) NOT NULL,
    birth_date DATE NOT NULL,
    age TINYINT UNSIGNED NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    salary_usd DECIMAL(10,2) NOT NULL,
    is_employed BOOLEAN NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL
);

LOAD DATA INFILE '/docker-entrypoint-initdb.d/data/users.csv'
INTO TABLE users
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    person_id,
    first_name,
    last_name,
    @middle_name,
    gender,
    birth_date,
    age,
    email,
    phone_number,
    country,
    city,
    company_name,
    salary_usd,
    @is_employed,
    created_at,
    updated_at
)
SET
    middle_name = NULLIF(@middle_name, ''),
    is_employed = CASE
        WHEN @is_employed = 'True' THEN 1
        ELSE 0
    END;