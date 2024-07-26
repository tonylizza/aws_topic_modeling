import os
import re
import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import chardet
import concurrent.futures

# Database connection details
endpoint = os.getenv('RDS_ENDPOINT')
db_password = os.getenv('DB_PASSWORD')
s3_bucket = os.getenv('S3_BUCKET_RAW')
s3_directory = os.getenv('S3_DIRECTORY', 'Part1/awards_1990/awd_1990_00')  # Specify the directory within the bucket
batch_size = 1000  # Adjust based on performance testing

# Initialize S3 client
s3 = boto3.client('s3')

# Clean data by removing null characters and other invalid characters
def clean_data(data):
    for key, value in data.items():
        if isinstance(value, str):
            data[key] = value.replace('\x00', '').replace('\ufffd', '')  # Remove null and replacement characters
    return data

def parse_award_file(file_content):
    data = {}
    patterns = {
        "title": r"Title\s+:\s+(.+)",
        "type": r"Type\s+:\s+(.+)",
        "nsf_org": r"NSF Org\s+:\s+(.+)",
        "latest_amendment_date": r"Latest\s+Amendment\s+Date\s+:\s+(.+)",
        "file": r"File\s+:\s+(.+)",
        "award_number": r"Award Number\s*:\s+(.+)",
        "award_instr": r"Award Instr\.\s*:\s+(.+)",
        "prgm_manager": r"Prgm Manager\s*:\s+(.+)",
        "start_date": r"Start Date\s+:\s+(.+)",
        "expires": r"Expires\s+:\s+(.+)",
        "expected_total_amt": r"Expected\s+Total Amt\.\s+:\s+\$(.+)\s+\(Estimated\)",
        "investigator": r"Investigator\s*:\s+(.+)",
        "abstract": r"Abstract\s+:\s+(.+)",
        "nsf_program": r"NSF Program\s+:\s+(.+)",
        "fld_applictn": r"Fld Applictn\s*:\s+(.+)",
        "program_ref": r"Program Ref\s+:\s+(.+)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, file_content, re.MULTILINE)
        if match:
            data[key] = match.group(1).strip()
        else:
            data[key] = None
            print(f"Pattern not found for {key}")

    # Extract sponsor information
    sponsor_match = re.search(
        r"Sponsor\s*:\s*(.*?)\n\s*(.*?)\n\s*(.*?)(\d{3}/\d{3}-\d{4})", file_content, re.MULTILINE
    )
    if sponsor_match:
        data["sponsor"] = sponsor_match.group(1).strip()
        data["sponsor_address"] = f"{sponsor_match.group(2).strip()}, {sponsor_match.group(3).strip()}"
        data["sponsor_phone"] = sponsor_match.group(4).strip()
    else:
        data["sponsor"] = None
        data["sponsor_address"] = None
        data["sponsor_phone"] = None
        print("Sponsor information not found.")

    # Parsing date fields
    for date_field in ['latest_amendment_date', 'start_date', 'expires']:
        if data[date_field]:
            # Remove any extra text after the date
            cleaned_date = re.sub(r'\s+\(.*\)', '', data[date_field])
            try:
                data[date_field] = datetime.strptime(cleaned_date, '%B %d, %Y').date()
            except ValueError as e:
                print(f"Error parsing date for {date_field}: {cleaned_date}, error: {e}")
                data[date_field] = None

    # Convert expected_total_amt to float
    if data['expected_total_amt']:
        data['expected_total_amt'] = float(data['expected_total_amt'].replace(',', ''))

    return clean_data(data)

def load_data_to_rds(records):
    conn = psycopg2.connect(
        dbname='nsf_awards_db',
        user='awarddbuser',
        password=db_password,
        host=endpoint,
        port='5432'
    )
    cur = conn.cursor()

    insert_award_query = '''
    INSERT INTO nsf_awards (title, type, nsf_org, latest_amendment_date, file, award_number, award_instr, prgm_manager, start_date, expires, expected_total_amt, abstract)
    VALUES %s
    RETURNING id;
    '''
    award_values = [
        (rec['title'], rec['type'], rec['nsf_org'], rec['latest_amendment_date'], rec['file'],
         rec['award_number'], rec['award_instr'], rec['prgm_manager'], rec['start_date'],
         rec['expires'], rec['expected_total_amt'], rec['abstract'])
        for rec in records
    ]

    print("Prepared award values for batch insert:")
    for value in award_values:
        print(value)  # Debugging statement to check the structure

    try:
        execute_values(cur, insert_award_query, award_values)
        award_ids = cur.fetchall()

        for award_id, rec in zip(award_ids, records):
            award_id = award_id[0]

            if rec['investigator']:
                investigators = rec['investigator'].split('\n')
                for inv in investigators:
                    insert_investigator_query = '''
                    INSERT INTO investigators (name, role, award_id)
                    VALUES (%s, %s, %s);
                    '''
                    name, role = inv.split('(')
                    role = role.replace(')', '').strip()
                    cur.execute(insert_investigator_query, (name.strip(), role, award_id))

            if rec['sponsor']:
                insert_sponsor_query = '''
                INSERT INTO sponsors (name, address, phone, award_id)
                VALUES (%s, %s, %s, %s);
                '''
                cur.execute(insert_sponsor_query, (rec['sponsor'], rec['sponsor_address'], rec['sponsor_phone'], award_id))

            if rec['nsf_program']:
                programs = rec['nsf_program'].split('\n')
                for program in programs:
                    insert_program_query = '''
                    INSERT INTO nsf_programs (code, name, award_id)
                    VALUES (%s, %s, %s);
                    '''
                    parts = program.split(maxsplit=1)
                    if len(parts) == 2:
                        code, name = parts
                    else:
                        code = parts[0]
                        name = ''
                    cur.execute(insert_program_query, (code, name, award_id))

            if rec['fld_applictn']:
                fields = rec['fld_applictn'].split('\n')
                for field in fields:
                    insert_field_query = '''
                    INSERT INTO field_applications (code, name, award_id)
                    VALUES (%s, %s, %s);
                    '''
                    parts = field.split(maxsplit=1)
                    if len(parts) == 2:
                        code, name = parts
                    else:
                        code = parts[0]
                        name = ''
                    cur.execute(insert_field_query, (code, name, award_id))

            if rec['program_ref']:
                references = rec['program_ref'].split(',')
                for ref in references:
                    insert_ref_query = '''
                    INSERT INTO program_refs (reference, award_id)
                    VALUES (%s, %s);
                    '''
                    cur.execute(insert_ref_query, (ref.strip(), award_id))

        conn.commit()
    except psycopg2.Error as e:
        print(f"Batch insert error: {e}")
        conn.rollback()
        # Attempt to insert records individually
        for rec in records:
            try:
                cur.execute(insert_award_query, rec)
                award_id = cur.fetchone()[0]

                if rec['investigator']:
                    investigators = rec['investigator'].split('\n')
                    for inv in investigators:
                        insert_investigator_query = '''
                        INSERT INTO investigators (name, role, award_id)
                        VALUES (%s, %s, %s);
                        '''
                        name, role = inv.split('(')
                        role = role.replace(')', '').strip()
                        cur.execute(insert_investigator_query, (name.strip(), role, award_id))

                if rec['sponsor']:
                    insert_sponsor_query = '''
                    INSERT INTO sponsors (name, address, phone, award_id)
                    VALUES (%s, %s, %s, %s);
                    '''
                    cur.execute(insert_sponsor_query, (rec['sponsor'], rec['sponsor_address'], rec['sponsor_phone'], award_id))

                if rec['nsf_program']:
                    programs = rec['nsf_program'].split('\n')
                    for program in programs:
                        insert_program_query = '''
                        INSERT INTO nsf_programs (code, name, award_id)
                        VALUES (%s, %s, %s);
                        '''
                        parts = program.split(maxsplit=1)
                        if len(parts) == 2:
                            code, name = parts
                        else:
                            code = parts[0]
                            name = ''
                        cur.execute(insert_program_query, (code, name, award_id))

                if rec['fld_applictn']:
                    fields = rec['fld_applictn'].split('\n')
                    for field in fields:
                        insert_field_query = '''
                        INSERT INTO field_applications (code, name, award_id)
                        VALUES (%s, %s, %s);
                        '''
                        parts = field.split(maxsplit=1)
                        if len(parts) == 2:
                            code, name = parts
                        else:
                            code = parts[0]
                            name = ''
                        cur.execute(insert_field_query, (code, name, award_id))

                if rec['program_ref']:
                    references = rec['program_ref'].split(',')
                    for ref in references:
                        insert_ref_query = '''
                        INSERT INTO program_refs (reference, award_id)
                        VALUES (%s, %s);
                        '''
                        cur.execute(insert_ref_query, (ref.strip(), award_id))

                conn.commit()
            except psycopg2.Error as e:
                print(f"Error inserting record: {e}")
                conn.rollback()

    cur.close()
    conn.close()

def process_s3_objects(bucket, keys):
    records = []
    for key in keys:
        print(f"Processing {key}")
        if key.endswith('.html'):
            print(f"Skipping file {key} because it is an HTML file.")
            continue

        s3_object = s3.get_object(Bucket=bucket, Key=key)
        raw_content = s3_object['Body'].read()

        # Detect encoding
        result = chardet.detect(raw_content)
        encoding = result['encoding']
        print(f"Detected encoding: {encoding}")

        if not encoding:
            print(f"Skipping file {key} because encoding could not be detected.")
            continue

        try:
            file_content = raw_content.decode(encoding)
            award_data = parse_award_file(file_content)
            if not any(award_data.values()):
                print(f"Skipping file {key} because it does not contain expected patterns.")
                continue
            records.append(award_data)
        except UnicodeDecodeError as e:
            print(f"Skipping file {key} due to decode error: {e}")

    if records:
        load_data_to_rds(records)

def main():
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    prefix = s3_directory if s3_directory else ''
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page['Contents']:
            keys.append(obj['Key'])

    # Split keys into batches
    key_batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_s3_objects, s3_bucket, batch) for batch in key_batches]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing batch: {e}")

if __name__ == '__main__':
    main()
