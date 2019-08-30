import csv
import io
import logging
import multiprocessing as mp
import os
import queue
import sys
import time
import zipfile

import psycopg2

# Basic logger setup
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel("INFO")

# Maximum number of times to retry connecting to the database on startup
LIMIT_DB_CONNECT_RETRIES = 10

###
# Record processing functions
###

def parseRow(row):
    """ Parse a single row of records, extracting the relevant fields

    Args:
        row: a single row

    Return:
        A dict with the extracted record
    """
    try:
        record = {
            "lmk_key": row['LMK_KEY'],
            "lodgement_date": row['LODGEMENT_DATE'],
            "transaction_type": row['TRANSACTION_TYPE'],
            "total_floor_area": row['TOTAL_FLOOR_AREA'],
            "addtess": row['ADDRESS'],
            "postcode": row['POSTCODE'],
        }
    except KeyError:
        record = None
    return record

def processRow(input_queue, record_queue):
    """ Process a single row of input, and pass it on to record handling queue

    Args:
        input_queue: a queue with lines of records to process (joinable)
        record_queue: a queue to submit processed record (dicts) to

    Return:
        None
    """
    while True:
        try:
            row = input_queue.get(True, 10)
            record = parseRow(row)
            if record:
                # If there's a successfully extracted record continue

                # Simulate long-ish API call
                time.sleep(0.250)

                # Put the record onto the record queue to be added to the database
                record_queue.put(record)
            # Feed back to the input queue for job counting
            input_queue.task_done()
        except queue.Empty:
            # Keep waiting for data to process
            pass

###
# Database functions
###

def connect_db():
    """ Connect to the external database

    Args:
        None

    Environment variables:
        POSTGRES_USER: username to connect with
        POSTGRES_PASSWORD: password to connect with
        POSTGRES_DB: database name to connect to

    Globals:
        LIMIT_DB_CONNECT_RETRIES: maxium count to retry (with delay)

    Return:
        An establised connection
    """
    conn = None
    retry_counter = 0
    while not conn:
        try:
            conn = psycopg2.connect(user = os.getenv("POSTGRES_USER"),
                                    password = os.getenv("POSTGRES_PASSWORD"),
                                    host = "db",
                                    database = os.getenv("POSTGRES_DB"))
        except psycopg2.OperationalError:
            if retry_counter >= LIMIT_DB_CONNECT_RETRIES:
                raise
            retry_counter += 1
            time.sleep(5)
    return conn

def create_tables(conn):
    """ Create tables for the data import database if
    the do not exist

    Args:
        conn: an established connection

    Return:
        None
    """
    command = """
        CREATE TABLE epc (
            lmk_key VARCHAR(255) PRIMARY KEY,
            lodgement_date DATE,
            transaction_type VARCHAR(255),
            total_foor_area FLOAT,
            address VARCHAR(255),
            postcode VARCHAR(255)
        )
        """
    try:
        cur = conn.cursor()
        # create table
        cur.execute(command)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        if error.__class__.__name__ == "DuplicateTable":
            logger.debug("Database table already exists.")
        else:
            raise(error)

def create_records(record_queue, conn):
    """ Worker process to add pre-processed records to the database

    Args:
        record_queue: a queue to pop off items to put into the database (joinable)
        conn: an established database connection

    Return:
        None
    """
    # The import SQL, if there's an item with the given 'lmk_key', ignore
    sql = """
        INSERT INTO epc (lmk_key, lodgement_date, transaction_type, total_foor_area, address, postcode)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (lmk_key)
        DO NOTHING;
    """
    # # An alternate version, to update any record that is matched with the given key
    # sql = """
    #     INSERT INTO epc (lmk_key, lodgement_date, transaction_type, total_foor_area, address, postcode)
    #     VALUES (%s, %s, %s, %s, %s, %s)
    #     ON CONFLICT (lmk_key)
    #     DO
    #         UPDATE
    #         SET lodgement_date = EXCLUDED.lodgement_date,
    #             transaction_type = EXCLUDED.transaction_type,
    #             total_foor_area = EXCLUDED.total_foor_area,
    #             address = EXCLUDED.address,
    #             postcode = EXCLUDED.postcode;
    # """

    # Start the worker process
    while True:
        try:
            # pop off a record from the queue if there's one
            record = record_queue.get(True, 10)
            try:
                # Add item to the database
                cur = conn.cursor()
                cur.execute(sql, (record['lmk_key'], record['lodgement_date'], record['transaction_type'], record['total_floor_area'], record['addtess'], record['postcode']))
                cur.close()
            except (Exception, psycopg2.OperationalError) as error:
                raise(error)
            finally:
                # Feed back to the queue for job counting
                record_queue.task_done()
        except queue.Empty:
            # Just wait for new tasks
            pass

def archive_enqueue(import_file, processing_queue):
    """ Read the input CSV from a zip file and push lines onto the processing queue

    Args:
        import_file: file to load (zip)
        processing_queue: where to enqueue the read lines for further processing

    Environment variables:
        MAXRECORDS: number of records to process, if 0 then all (default 0)

    Return:
        The number of records enqueued
    """
    # For this exercise, potentially limit the number of records to process
    # This is to set a manageable limit, specific for this case
    max_records = int(os.getenv("MAXRECORDS", 0))
    max_records_reached = False
    counter=0

    logger.info(f'Import file: {import_file}')
    input_archive = zipfile.ZipFile(import_file)
    for filename in input_archive.namelist():
        logger.info(f'Assessing file: {filename}')
        if os.path.basename(filename) == "certificates.csv":
            with input_archive.open(filename) as csvfile:
                items_file = io.TextIOWrapper(csvfile)
                reader = csv.DictReader(items_file, delimiter=',', quotechar='"')
                for row in reader:
                    counter += 1
                    processing_queue.put(row)
                    if max_records > 0 and counter >= max_records:
                        max_records_reached = True
                        break
            if max_records_reached:
                break
    return counter

def main(import_file):
    """ The main process of data processing task

    Args:
        import_file: file to load items from

    Environment variables:
        THREADS: number of workers in the processing worker pool (default 100)

    Return
        None
    """
    # Set up database connection
    conn = connect_db()
    # Enable AUTOCOMMIT for direct writes
    conn.set_session(autocommit=True)
    # Create the requited database tables if they don't exists yet
    create_tables(conn)
    

    ## Task management setup
    # Database queue and worker setup
    db_queue = mp.JoinableQueue()
    db_process = mp.Process(target=create_records, args=(db_queue,conn))
    db_process.start()

    # Imput processor queue and worker pool setup
    processing_queue = mp.JoinableQueue()
    processing_pool = mp.Pool(int(os.getenv("THREADS", 100)), processRow, (processing_queue,db_queue))

    total_records = archive_enqueue(import_file, processing_queue)
                
    logger.info("Processing enqueued tasks")
    while True:
        # Check queue sizes, and display them in the logs
        qsize = processing_queue.qsize()
        qsize2 = db_queue.qsize()
        logger.info(f'Input queue: {qsize} | database queue: {qsize2}')
        if qsize == 0 and qsize2 == 0:
            # When queues finished, stop looping
            break
        time.sleep(1)

    # Finish and clean up queues, workers, and connections
    processing_queue.join()
    processing_pool.close()
    db_queue.join()
    db_process.terminate()
    conn.close()

    # Job results
    logger.info(f'Processed {total_records} records')

if __name__ == "__main__":
    try:
        import_file=sys.argv[1]
    except IndexError:
        print("Input file argument not passed", file=sys.stderr)
        raise

    main(import_file)