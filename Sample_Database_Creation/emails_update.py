import psycopg
import os

from dotenv import load_dotenv
load_dotenv()

# 1. Email list (must match number of rows you want to update)
emails= ['satish17.amara@gmail.com', 
'kumar17.amara@gmail.com',
'saisatisha.be23@uceou.edu',
'upatanjali@gmail.com',
'upatanjali6@gmail.com',
'123003254@sastra.ac.in',
'tsmentjhon@gmail.com',
'gaganvad16@gmail.com',
'databloggers01@gmail.com',
'20bds019@iiitdwd.ac.in',
'vadlamudinaveena2@gmail.com',
'aryanrachala23@gmail.com',
'bunnycudde52@gmail.com',
'aryanrachala54@gmail.com',
'namara@buffalo.edu']

# 2. DB connection
conn = psycopg.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "refunds_db"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD"),
)

try:
    with conn:
        with conn.cursor() as cur:
            # 3. Fetch IDs of rows to update (ORDER MATTERS)
            cur.execute("""
                SELECT customer_id
                FROM customers
                ORDER BY customer_id
                LIMIT %s
            """, (len(emails),))

            rows = cur.fetchall()

            if len(rows) != len(emails):
                raise ValueError("Number of emails does not match number of rows")

            # 4. Assign emails to rows
            update_data = [
                (email, row_id[0])
                for email, row_id in zip(emails, rows)
            ]

            cur.executemany("""
                UPDATE customers
                SET customer_email = %s
                WHERE customer_id = %s
            """, update_data)

    print("Emails successfully assigned!")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    conn.close()
