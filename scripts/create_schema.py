import psycopg2
import os

def create_schema():
    # Read the endpoint from the file
    with open('rds_endpoint.txt', 'r') as f:
        endpoint = f.read().strip()

    conn = psycopg2.connect(
        dbname='nsf_awards_db',
        user='mymasteruser',
        password='mypassword',
        host=endpoint,
        port='5432'
    )
    cur = conn.cursor()

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS nsf_awards (
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

    CREATE TABLE IF NOT EXISTS investigators (
        id SERIAL PRIMARY KEY,
        name TEXT,
        role TEXT,
        award_id INTEGER REFERENCES nsf_awards(id)
    );

    CREATE TABLE IF NOT EXISTS sponsors (
        id SERIAL PRIMARY KEY,
        name TEXT,
        address TEXT,
        phone TEXT,
        award_id INTEGER REFERENCES nsf_awards(id)
    );

    CREATE TABLE IF NOT EXISTS nsf_programs (
        id SERIAL PRIMARY KEY,
        code TEXT,
        name TEXT,
        award_id INTEGER REFERENCES nsf_awards(id)
    );

    CREATE TABLE IF NOT EXISTS field_applications (
        id SERIAL PRIMARY KEY,
        code TEXT,
        name TEXT,
        award_id INTEGER REFERENCES nsf_awards(id)
    );

    CREATE TABLE IF NOT EXISTS program_refs (
        id SERIAL PRIMARY KEY,
        reference TEXT,
        award_id INTEGER REFERENCES nsf_awards(id)
    );
    '''
    
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_schema()
