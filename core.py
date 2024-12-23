import psycopg2
import mysql.connector
import os
import pymongo

class Backup:
    def __init__(self, host, port = 3306, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def mysql(self, databases, backup_dir):
        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )
        self._backup_mysql_databases(databases, backup_dir)
        self.conn.close()

    def postgres(self, databases, backup_dir):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )
        self._backup_postgres_databases(databases, backup_dir, conn)
        conn.close()

    def mongodb(self, databases, backup_dir):
        user=self.user
        password=self.password
        host=self.host
        port=self.port
        uri = f"mongodb://{user}:{password}@{host}:{port}"
        client = pymongo.MongoClient(uri)
        self._backup_mongodb_databases(databases, backup_dir, client)

    def _backup_mysql_databases(self, databases, backup_dir):
        cursor = self.conn.cursor()
        for db in databases:
            backup_file = os.path.join(backup_dir, f"{db}.sql")
            cursor.execute("SHOW CREATE DATABASE " + db)
            db_create_query = cursor.fetchone()[1]
            with open(backup_file, "a") as f:
                f.write(db_create_query + "\n\n")
            cursor.execute("SHOW table STATUS FROM " + db)
            for tbl in cursor.fetchall():
                table_create_query = "SHOW CREATE TABLE " + db + "." + tbl[0] + ";"
                cursor.execute(table_create_query)
                create_table_query = cursor.fetchone()[1] + ";\n"
                with open(backup_file, "a") as f:
                    f.write(create_table_query)
                insert_query = f"SELECT * FROM {db}.{tbl[0]};\n"
                with open(backup_file, "a") as f:
                    f.write(insert_query)

    def _backup_postgres_databases(self, databases, backup_dir, conn):
        cursor = conn.cursor()
        for db in databases:
            backup_file = os.path.join(backup_dir, f"{db}.sql")

            cursor.execute(f"SELECT datname FROM pg_database WHERE datname = '{db}'")
            if cursor.rowcount == 0:
                print(f"Database '{db}' does not exist.")
                continue

            cursor.execute("SET search_path TO " + db)

            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema='public'
                ORDER BY table_type, table_name
            """)
            table_list = [row[0] for row in cursor.fetchall()]

            with open(backup_file, "w") as f:
                f.write(f"-- PostgreSQL dump from {db}\n\n")

                for table in table_list:
                    create_table_query = f"SELECT format('%s', tablename, tableowner, tablespace, tabletype, '', reloptions, array_agg(relacl)) FROM pg_tables WHERE tablename = '{table}';"
                    cursor.execute(create_table_query)
                    row = cursor.fetchone()
                    f.write(f"{row[0]};\n\n")

                    select_query = f"SELECT * FROM {table}"
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    for row in rows:
                        f.write(f"INSERT INTO {table} VALUES {row};\n")

    def _backup_mongodb_databases(self, databases, backup_dir, conn):
        for db_name in databases:
            db = conn[db_name]
            backup_file = os.path.join(backup_dir, f"{db_name}.bson")

            collections = db.list_collection_names()
            with open(backup_file, "wb") as outfile:
                for collection in collections:
                    collection_data = db[collection].find()
                    for document in collection_data:
                        document_json = json.dumps(document).encode('utf-8')
                        outfile.write(document_json)
                        outfile.write(b'\n')
                        print(f"Backing up {db_name}.{collection}")

