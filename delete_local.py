import psycopg2

def clear_database():
    endpoint = 'localhost'  # Localhost for local PostgreSQL
    db_password = 'local_password'  # Replace with your actual local DB password

    conn = psycopg2.connect(
        dbname='local_awards_db',
        user='local_awarddbuser',
        password=db_password,
        host=endpoint,
        port='5432'
    )
    cur = conn.cursor()

    truncate_table_queries = [
        "TRUNCATE TABLE investigators CASCADE;",
        "TRUNCATE TABLE sponsors CASCADE;",
        "TRUNCATE TABLE nsf_programs CASCADE;",
        "TRUNCATE TABLE field_applications CASCADE;",
        "TRUNCATE TABLE program_refs CASCADE;",
        "TRUNCATE TABLE nsf_awards CASCADE;"
    ]

    try:
        for query in truncate_table_queries:
            cur.execute(query)
        conn.commit()
        print("All tables have been cleared.")
    except psycopg2.Error as e:
        print(f"Error clearing tables: {e}")
        conn.rollback()

    cur.close()
    conn.close()

if __name__ == '__main__':
    clear_database()
