import psycopg2


def create_tables(conn):
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE epc (
            lmk_key VARCHAR(255) PRIMARY KEY,
            lodgement_date DATE,
            transaction_type VARCHAR(255),
            total_foor_area FLOAT,
            address VARCHAR(255),
            postcode VARCHAR(255)
        )
        """,
    )
    try:
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

conn = psycopg2.connect(host="db",database="imports", user="postgres", password="somethingrandom")
create_tables(conn)

# cur = conn.cursor()
# sql = """INSERT INTO vendors(vendor_name)
#              VALUES(%s) RETURNING vendor_id;"""
# sql="""
# INSERT INTO epc (lmk_key, total_foor_area)
# VALUES (%s, %s)
# ON CONFLICT (lmk_key)
# DO
#     UPDATE
#     SET total_foor_area = EXCLUDED.total_foor_area;
# """
# cur.execute(sql, ('5678', 23))
# cur.close()
# conn.commit()