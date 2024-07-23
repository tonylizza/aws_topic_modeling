import os
import re
import psycopg2
from datetime import datetime

endpoint = os.getenv('RDS_ENDPOINT')
db_password = os.getenv('DB_PASSWORD')

def parse_award_file(file_content):
    data = {}
    patterns = {
        "title": r"Title\s+:\s+(.*)",
        "type": r"Type\s+:\s+(.*)",
        "nsf_org": r"NSF Org\s+:\s+(.*)",
        "latest_amendment_date": r"Latest\s+Amendment\s+Date\s+:\s+(.*)",
        "file": r"File\s+:\s+(.*)",
        "award_number": r"Award Number\s+:\s+(.*)",
        "award_instr": r"Award Instr\.\s+:\s+(.*)",
        "prgm_manager": r"Prgm Manager\s+:\s+(.*)",
        "start_date": r"Start Date\s+:\s+(.*)",
        "expires": r"Expires\s+:\s+(.*)",
        "expected_total_amt": r"Expected\s+Total Amt\.\s+:\s+\$(.*)\s+\(Estimated\)",
        "investigator": r"Investigator\s+:\s+(.*)",
        "sponsor": r"Sponsor\s+:\s+(.*)",
        "sponsor_address": r"Sponsor\s+:\s+.*\n\s+(.*)",
        "sponsor_phone": r"Sponsor\s+:\s+.*\n\s+.*\n\s+(.*)",
        "nsf_program": r"NSF Program\s+:\s+(.*)",
        "fld_applictn": r"Fld Applictn\s+:\s+(.*)",
        "program_ref": r"Program Ref\s+:\s+(.*)",
        "abstract": r"Abstract\s+:\s+(.*)"
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, file_content, re.MULTILINE)
        data[key] = match.group(1).strip() if match else None

    # Parsing date fields
    for date_field in ['latest_amendment_date', 'start_date', 'expires']:
        if data[date_field]:
            data[date_field] = datetime.strptime(data[date_field], '%B %d, %Y').date()

    # Convert expected_total_amt to float
    if data['expected_total_amt']:
        data['expected_total_amt'] = float(data['expected_total_amt'].replace(',', ''))

    return data

def load_data_to_rds(data):
    conn = psycopg2.connect(
        dbname='nsf_awards_db',
        user='awarddbuser',
        password=db_password,
        host=endpoint,
        port='5432'
    )
    cur = conn.cursor()

    # Insert into nsf_awards table
    insert_award_query = '''
    INSERT INTO nsf_awards (title, type, nsf_org, latest_amendment_date, file, award_number, award_instr, prgm_manager, start_date, expires, expected_total_amt, abstract)
    VALUES (%(title)s, %(type)s, %(nsf_org)s, %(latest_amendment_date)s, %(file)s, %(award_number)s, %(award_instr)s, %(prgm_manager)s, %(start_date)s, %(expires)s, %(expected_total_amt)s, %(abstract)s)
    RETURNING id;
    '''
    print("Executing insert_award_query:", insert_award_query)
    cur.execute(insert_award_query, data)
    award_id = cur.fetchone()[0]
    print("Inserted award_id:", award_id)  # Debugging statement

    # Insert into investigators table
    if data['investigator']:
        investigators = data['investigator'].split('\n')
        for inv in investigators:
            insert_investigator_query = '''
            INSERT INTO investigators (name, role, award_id)
            VALUES (%s, %s, %s);
            '''
            name, role = inv.split('(')
            role = role.replace(')', '').strip()
            print("Executing insert_investigator_query:", insert_investigator_query, (name.strip(), role, award_id))  # Debugging statement
            cur.execute(insert_investigator_query, (name.strip(), role, award_id))

    # Insert into sponsors table
    if data['sponsor']:
        insert_sponsor_query = '''
        INSERT INTO sponsors (name, address, phone, award_id)
        VALUES (%s, %s, %s, %s);
        '''
        cur.execute(insert_sponsor_query, (data['sponsor'], data['sponsor_address'], data['sponsor_phone'], award_id))

    # Insert into nsf_programs table
    if data['nsf_program']:
        programs = data['nsf_program'].split('\n')
        for program in programs:
            insert_program_query = '''
            INSERT INTO nsf_programs (code, name, award_id)
            VALUES (%s, %s, %s);
            '''
            code, name = program.split()
            print("Executing insert_program_query:", insert_program_query, (code, name, award_id))  # Debugging statement
            cur.execute(insert_program_query, (code, name, award_id))

    # Insert into field_applications table
    if data['fld_applictn']:
        fields = data['fld_applictn'].split('\n')
        for field in fields:
            insert_field_query = '''
            INSERT INTO field_applications (code, name, award_id)
            VALUES (%s, %s, %s);
            '''
            code, name = field.split()
            print("Executing insert_field_query:", insert_field_query, (code, name, award_id))  # Debugging statement
            cur.execute(insert_field_query, (code, name, award_id))

    # Insert into program_refs table
    if data['program_ref']:
        references = data['program_ref'].split(',')
        for ref in references:
            insert_ref_query = '''
            INSERT INTO program_refs (reference, award_id)
            VALUES (%s, %s);
            '''
            print("Executing insert_ref_query:", insert_ref_query, (ref.strip(), award_id))  # Debugging statement
            cur.execute(insert_ref_query, (ref.strip(), award_id))

    conn.commit()
    cur.close()
    conn.close()

def process_award_files(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            with open(os.path.join(root, file), 'r') as f:
                file_content = f.read()
                award_data = parse_award_file(file_content)
                load_data_to_rds(award_data)

# Specify the directory containing the award files
process_award_files('../data')
