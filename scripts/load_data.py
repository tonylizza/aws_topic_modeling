import os
import re
import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import chardet
import concurrent.futures
import logging
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
endpoint = os.getenv('RDS_ENDPOINT')
db_password = os.getenv('DB_PASSWORD')
s3_bucket = os.getenv('S3_BUCKET_RAW')
s3_directory = os.getenv('S3_DIRECTORY', '')
batch_size = 100  # Reduced batch size

# Initialize S3 client
s3 = boto3.client('s3')

def clean_data(data):
    return {k: v.replace('\x00', '').replace('\ufffd', '') if isinstance(v, str) else v for k, v in data.items()}

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
        "abstract": r"Abstract\s+:\s+([\s\S]+)",
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
            logging.warning(f"Pattern not found for {key}")

    sponsor_match = re.search(
        r"Sponsor\s*:\s*(.*?)\n\s*(.*?)\n\s*(.*?)(\d{3}/\d{3}-\d{4})", file_content, re.MULTILINE
    )
    if sponsor_match:
        data["sponsor"] = sponsor_match.group(1).strip()
        data["sponsor_address"] = f"{sponsor_match.group(2).strip()}, {sponsor_match.group(3).strip()}"
        data["sponsor_phone"] = sponsor_match.group(4).strip()
    else:
        data["sponsor"] = data["sponsor_address"] = data["sponsor_phone"] = None
        logging.warning("Sponsor information not found.")

    if data["abstract"]:
        data["abstract"] = re.sub(r'\s+', ' ', data["abstract"])
        data["abstract"] = re.sub(r'[-=]{5,}', '', data["abstract"])
        data["abstract"] = re.sub(r'^\d{7}\s+', '', data["abstract"])

    for date_field in ['latest_amendment_date', 'start_date', 'expires']:
        if data[date_field]:
            cleaned_date = re.sub(r'\s+\(.*\)', '', data[date_field])
            try:
                data[date_field] = datetime.strptime(cleaned_date, '%B %d, %Y').date()
            except ValueError as e:
                logging.error(f"Error parsing date for {date_field}: {cleaned_date}, error: {e}")
                data[date_field] = None

    if data['expected_total_amt']:
        try:
            data['expected_total_amt'] = float(data['expected_total_amt'].replace(',', ''))
        except ValueError:
            logging.error(f"Error converting expected_total_amt to float: {data['expected_total_amt']}")
            data['expected_total_amt'] = None

    return clean_data(data)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname='nsf_awards_db',
            user='awarddbuser',
            password=db_password,
            host=endpoint,
            port='5432'
        )
        logging.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def load_data_to_rds(records):
    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()

    try:
        # Wrap each major operation in its own try-except block

        try:
            # Batch insert for nsf_awards table
            award_values = [
                (rec['title'], rec['type'], rec['nsf_org'], rec['latest_amendment_date'], rec['file'],
                 rec['award_number'], rec['award_instr'], rec['prgm_manager'], rec['start_date'],
                 rec['expires'], rec['expected_total_amt'], rec['abstract'])
                for rec in records
            ]
            
            execute_values(cur, """
                INSERT INTO nsf_awards (title, type, nsf_org, latest_amendment_date, file, award_number, 
                                        award_instr, prgm_manager, start_date, expires, expected_total_amt, abstract)
                VALUES %s
                ON CONFLICT (award_number) DO UPDATE SET
                    title = EXCLUDED.title,
                    type = EXCLUDED.type,
                    nsf_org = EXCLUDED.nsf_org,
                    latest_amendment_date = EXCLUDED.latest_amendment_date,
                    file = EXCLUDED.file,
                    award_instr = EXCLUDED.award_instr,
                    prgm_manager = EXCLUDED.prgm_manager,
                    start_date = EXCLUDED.start_date,
                    expires = EXCLUDED.expires,
                    expected_total_amt = EXCLUDED.expected_total_amt,
                    abstract = EXCLUDED.abstract
                RETURNING id, award_number;
            """, award_values)
            
            award_id_map = {award_number: id for id, award_number in cur.fetchall()}
            logging.info(f"Inserted or updated {len(award_id_map)} awards in batch")
        except Exception as e:
            logging.error(f"Error inserting into nsf_awards table: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

        try:
            # Batch insert for investigators
            all_investigators = set()
            for record in records:
                if record['investigator']:
                    all_investigators.update(name.split('(')[0].strip() for name in record['investigator'].split('\n'))
            
            inv_values = [(name,) for name in all_investigators]
            execute_values(cur, """
                INSERT INTO investigators (name)
                VALUES %s
                ON CONFLICT (name) DO NOTHING
                RETURNING id, name;
            """, inv_values)
            investigator_id_map = {name: id for id, name in cur.fetchall()}
        except Exception as e:
            logging.error(f"Error inserting into investigators table: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

        try:
            # Batch insert for award_investigators
            award_inv_values = []
            for record in records:
                award_id = award_id_map[record['award_number']]
                if record['investigator']:
                    for inv in record['investigator'].split('\n'):
                        name = inv.split('(')[0].strip()
                        role = inv.split('(')[1].replace(')', '').strip() if '(' in inv else ''
                        inv_id = investigator_id_map[name]
                        award_inv_values.append((award_id, inv_id, role))
            
            if award_inv_values:
                execute_values(cur, """
                    INSERT INTO award_investigators (award_id, investigator_id, role)
                    VALUES %s
                    ON CONFLICT (award_id, investigator_id) DO UPDATE SET role = EXCLUDED.role;
                """, award_inv_values)
        except Exception as e:
            logging.error(f"Error inserting into award_investigators table: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

        # Add similar try-except blocks for other tables (sponsors, nsf_programs, field_applications, etc.)

        conn.commit()
        logging.info(f"Successfully inserted batch of {len(records)} records")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting batch: {e}")
        logging.error(f"Detailed error: {str(e)}")
    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")
        
def process_s3_objects(bucket, keys):
    records = []
    for key in keys:
        logging.info(f"Processing {key}")
        if key.endswith('.html'):
            logging.info(f"Skipping file {key} because it is an HTML file.")
            continue

        try:
            s3_object = s3.get_object(Bucket=bucket, Key=key)
            raw_content = s3_object['Body'].read()

            result = chardet.detect(raw_content)
            encoding = result['encoding']
            logging.info(f"Detected encoding: {encoding}")

            if not encoding:
                logging.warning(f"Skipping file {key} because encoding could not be detected.")
                continue

            file_content = raw_content.decode(encoding)
            award_data = parse_award_file(file_content)
            if not any(award_data.values()):
                logging.warning(f"Skipping file {key} because it does not contain expected patterns.")
                continue
            records.append(award_data)
        except Exception as e:
            logging.error(f"Error processing file {key}: {e}")

    if records:
        load_data_to_rds(records)
    else:
        logging.warning("No valid records found to load into the database.")

def main():
    logging.info("Starting the data loading process")
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    prefix = s3_directory if s3_directory else ''
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])

    if not keys:
        logging.warning("No files found in the specified S3 bucket and directory.")
        return

    logging.info(f"Found {len(keys)} files to process")
    key_batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_s3_objects, s3_bucket, batch) for batch in key_batches]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing batch: {e}")

    logging.info("Data loading process completed")

if __name__ == '__main__':
    main()