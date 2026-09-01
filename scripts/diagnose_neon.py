import psycopg2

NEON_HOST = "ep-tiny-sunset-ah3xbklk-pooler.c-3.us-east-1.aws.neon.tech"
NEON_PORT = "5432"
NEON_DB = "neondb"
NEON_USER = "neondb_owner"
NEON_PASSWORD = "npg_nf0Bpoz8SKtF"  

conn = psycopg2.connect(
    host=NEON_HOST,
    port=NEON_PORT,
    dbname=NEON_DB,
    user=NEON_USER,
    password=NEON_PASSWORD,
    sslmode="require",
)
cur = conn.cursor()

print("== Current database ==")
cur.execute("SELECT current_database();")
print(cur.fetchone())

print("\n== All schemas in this database ==")
cur.execute("SELECT schema_name FROM information_schema.schemata;")
for row in cur.fetchall():
    print(row)

print("\n== All tables (any schema) named like our target tables ==")
cur.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name IN ('patients','doctors','appointments','treatments','billing')
    ORDER BY table_schema, table_name;
""")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(row)
else:
    print("No matching tables found in ANY schema. The tables genuinely don't exist yet.")

print("\n== Everything inside hmd schema specifically (if it exists) ==")
cur.execute("""
    SELECT table_name FROM information_schema.tables WHERE table_schema = 'hmd';
""")
rows = cur.fetchall()
print(rows if rows else "hmd schema exists but is empty, or doesn't exist at all.")

cur.close()
conn.close()