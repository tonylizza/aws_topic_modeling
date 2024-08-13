import psycopg2
import os

def create_schema():
    endpoint = os.getenv('RDS_ENDPOINT')
    db_password = os.getenv('DB_PASSWORD')

    conn = psycopg2.connect(
        dbname='nsf_awards_db',
        user='awarddbuser',
        password=db_password,
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
        name TEXT UNIQUE,
        role TEXT
    );

    CREATE TABLE IF NOT EXISTS award_investigators (
        award_id INTEGER REFERENCES nsf_awards(id),
        investigator_id INTEGER REFERENCES investigators(id),
        PRIMARY KEY (award_id, investigator_id)
    );

    CREATE TABLE IF NOT EXISTS sponsors (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        address TEXT,
        phone TEXT
    );

    CREATE TABLE IF NOT EXISTS award_sponsors (
        award_id INTEGER REFERENCES nsf_awards(id),
        sponsor_id INTEGER REFERENCES sponsors(id),
        PRIMARY KEY (award_id, sponsor_id)
    );

    CREATE TABLE IF NOT EXISTS nsf_programs (
        id SERIAL PRIMARY KEY,
        code TEXT,
        name TEXT
    );

    CREATE TABLE IF NOT EXISTS award_programs (
        award_id INTEGER REFERENCES nsf_awards(id),
        program_id INTEGER REFERENCES nsf_programs(id),
        PRIMARY KEY (award_id, program_id)
    );

    CREATE TABLE IF NOT EXISTS field_applications (
        id SERIAL PRIMARY KEY,
        code TEXT,
        name TEXT
    );

    CREATE TABLE IF NOT EXISTS award_field_applications (
        award_id INTEGER REFERENCES nsf_awards(id),
        field_application_id INTEGER REFERENCES field_applications(id),
        PRIMARY KEY (award_id, field_application_id)
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