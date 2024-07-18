CREATE DATABASE nsf_awards_db;

\c nsf_awards_db;

CREATE TABLE nsf_awards (
    id SERIAL PRIMARY KEY,
    title TEXT,
    type TEXT,
    nsf_org TEXT,
    latest_amendment_date DATE,
    file TEXT,
    award_number TEXT UNIQUE,
    award_instr TEXT,
    prgm_manager TEXT,
    start_date DATE,
    expires DATE,
    expected_total_amt NUMERIC(15, 2),
    abstract TEXT
);

CREATE TABLE investigators (
    id SERIAL PRIMARY KEY,
    name TEXT,
    role TEXT,
    award_id INTEGER REFERENCES nsf_awards(id)
);

CREATE TABLE sponsors (
    id SERIAL PRIMARY KEY,
    name TEXT,
    address TEXT,
    phone TEXT,
    award_id INTEGER REFERENCES nsf_awards(id)
);

CREATE TABLE nsf_programs (
    id SERIAL PRIMARY KEY,
    code TEXT,
    name TEXT,
    award_id INTEGER REFERENCES nsf_awards(id)
);

CREATE TABLE field_applications (
    id SERIAL PRIMARY KEY,
    code TEXT,
    name TEXT,
    award_id INTEGER REFERENCES nsf_awards(id)
);

CREATE TABLE program_refs (
    id SERIAL PRIMARY KEY,
    reference TEXT,
    award_id INTEGER REFERENCES nsf_awards(id)
);
