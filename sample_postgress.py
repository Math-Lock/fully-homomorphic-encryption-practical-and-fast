import sys
import arrow
import psycopg2
import sample_mathlock_rest as rest_sample
from typing import Generator


class MathLockPostgresSample:
    """
    Sample for PostgreSQL operations and REST API operations. Where fully homomorphic encryption/Decryption operations
    can be performed via REST API by our Flask server, and FHE math. operations will be executed on encrypted data
    in PostgreSQL by its C language based custom plugin, which has been implemented to perform Homomorphic arithmetic
    operations. Please take a note, that it's a Postgres installation on a dedicated VM for Demo purpose ONLY! The same
    database and VM may be used by other users, and maybe even simultaneously. So please make sure to created at least
    table with absolutely unique name to avoid cross/misuse.

    IMPORTANT: in case you like it and want better demo, you may request us to create specific DB user etc for better
    privacy and to avoid be affected by other users
    """
    def __init__(self, database="mathlock_db", host="46.4.106.106", user="math_lock", password="Afc13advc5sjyg!ysgd",
                 port="54141") -> None:
        self.conn = psycopg2.connect(database=database, host=host, user=user, password=password, port=port)
        self.cursor = self.conn.cursor()
        self.rest_sample = rest_sample.SampleMathLockApp()  # takes another sample instance

    # region public methods

    def create_extension(self, ext_name: str = "mathlock") -> None:
        """ The method creates custom, proprietary Postgres extension writen by Math-Lock. On our demo-server it's
        already compiled and created, so in fact this method here just for the sake of stability. """
        cmd = f"CREATE EXTENSION IF NOT EXISTS {ext_name}"
        self.cursor.execute(cmd)
        self.conn.commit()

    def get_all_table_names(self) -> Generator:
        """ The method gives all public table names available in the db """
        self.cursor.execute(f"select table_name from information_schema.tables where table_schema "
                            f"not in ('information_schema', 'pg_catalog') and table_type = 'BASE TABLE' order by "
                            f"table_name;")
        tables = self.cursor.fetchall()
        for table in tables:
            yield table[0]

    def get_all_tables_info(self) -> list:
        """ The method gives all public tables available in the db """
        self.cursor.execute(f"SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog'"
                            f" AND schemaname != 'information_schema'")
        return self.cursor.fetchall()

    def select_all_from_table(self, table_name: str, print_rows: bool = False) -> list or None:
        """ The method selects all from a given table """
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        if rows:
            if print_rows:
                for x in rows:
                    print(x)
            return rows

        print("Nothing to fetch, table is empty")
        return None

    def select_math_result_by_row_id(self, table_name: str, row_id: int) -> list:
        """ The method performs select by a record ID. Generally we store math. operations result into column N3 """
        self.cursor.execute(f"SELECT data3 FROM {table_name} WHERE id = {row_id}")
        res = self.cursor.fetchall()
        print(f"Fetched result by ID: {row_id}, from table: [{table_name}], ciphertext after math operation: {res}")

        return res

    def is_table_exists(self, table_name: str) -> bool:
        """ The method validates whether table exists or not """
        cmd = f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'"
        self.cursor.execute(cmd)
        info = self.cursor.fetchone()
        if info:
            # print("TRUE")
            return True

        # print("FALSE")
        return False

    def create_mathlock_table(self, table_name: str) -> bool:
        """ The method creates a table by given name """
        if not table_name:
            print(f"Table name is empty, terminating")
            sys.exit(1)
        if not self.is_table_exists(table_name):
            cmd = f"CREATE TABLE IF NOT EXISTS public.{table_name} (id integer NOT NULL, " \
                "data1 public.mathlock, data2 public.mathlock, data3 public.mathlock);"
            self.cursor.execute(cmd)
            self.conn.commit()
            res = self.is_table_exists(table_name)
            print(f"Table: [{table_name}] has been created successfully: {res}")
            return res

        print(f"Table: [{table_name}] already exists")
        return True

    def insert_into_table(self, table_name: str, index: int, m1: str, m2: str) -> None:
        """ The method performs insertion into given table for Column 1,2 by any index. Where column 1 and 2
        are ciphertext of Matrix 1 and ciphertext of Matrix 2 """
        cmd = f"INSERT INTO public.{table_name} (id, data1, data2) VALUES ({index}, '{m1}', '{m2}');"
        self.cursor.execute(cmd)
        self.conn.commit()

    def drop_mathlock_table(self, table_name: str) -> bool:
        """ The method drops existing table by given name """
        if not table_name:
            print(f"Table name is empty, terminating")
            sys.exit(1)

        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.commit()
        if not self.is_table_exists(table_name):
            print(f"Table: [{table_name}] has been deleted successfully")
            return True

        print(f"Table: [{table_name}] can't be deleted, or doesn't exists")
        return False

    def delete_row(self, table_name: str, row_id: int) -> int:
        """ The method deletes row (record) by given ID and returns amount of deleted rows """
        rows_deleted = 0
        try:
            self.cursor.execute(f"DELETE FROM {table_name} WHERE id = '{row_id}'")
            rows_deleted = self.cursor.rowcount
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as exc:
            print(f"Inner delete exception: {exc}")

        return rows_deleted

    def mult_matrices(self, table_name: str, row_id: int) -> None:
        """ The method performs FHE matrix multiplication inside Postgres by our custom extension, using given table name
         and row (record) ID """
        print(f"Called FHE Multiplication operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET data3 = data1 * data2 where id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def div_matrices(self, table_name, row_id) -> None:
        """ The method performs FHE matrix division inside Postgres by our custom extension, using given table name
        and row (record) ID """
        print(f"Called FHE Division operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET data3 = data1 / data2 where id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def add_matrices(self, table_name, row_id) -> None:
        """ The method performs FHE matrix addition inside Postgres by our custom extension, using given table name
         and row (record) ID """
        print(f"Called FHE Addition operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET data3 = data1 + data2 where id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def subtract_matrices(self, table_name, row_id) -> None:
        """ The method performs FHE matrix subtraction inside Postgres by our custom extension, using given table name
         and row (record) ID """
        print(f"Called FHE Subtraction operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET data3 = data1 - data2 where id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def execute_operation_in_a_loop(self, action: type, iterator: int, table_name: str, row_id: int) -> None:
        """ The method performs operation in a loop using for input required method's name, measuring performance """
        start_time = arrow.now()
        for i in range(iterator):
            action(table_name, row_id)
        end_time = float(f"{(arrow.now() - start_time).microseconds / 1000000:.5f}")
        print(f"Execution took: {end_time} seconds")

    def rest_do_encryption(self, value: [int, str]) -> dict:
        """ The method performs Fully Homomorphic Encryption operation using our other sample for REST API """
        return self.rest_sample.do_encryption(value)

    def rest_do_decryption(self, m1: dict) -> dict:
        """ The method performs Fully Homomorphic Decryption operation using our other sample for REST API """
        return self.rest_sample.do_decryption(m1)

    def build_string_for_postgres(self, value: dict) -> str:
        """ String builder to match to some specific syntax """
        return f"{'{'}{value['a']},{value['b']},{value['c']},{value['d']}{'}'}"

    def prepare_data_for_decryption(self, data: list) -> dict:
        """ The method prepares data for decryption via REST API """
        rebuild_string = str(data[0][0]).replace('{', '').replace('}', '').split(",", 4)
        return {"a": rebuild_string[0], "b": rebuild_string[1], "c": rebuild_string[2], "d": rebuild_string[3]}

    # endregion


if __name__ == "__main__":

    value1 = '10.5'
    value2 = '5.34'
    tab_name = "do_test_table"  # please make sure to define unique name of your table
    record_index = 1  # don't forget to change or increment index for your experiments, otherwise always == 1
    postgres_sample = MathLockPostgresSample()
    postgres_sample.create_extension()
    print(f"Original values to operate with are: {value1} and {value2}")

    # By default, we create only one record with index 1, every time deleting the table. If you want to avoid deletion
    # and to use multiple indexes - comment out deletion line and change the code accordingly
    postgres_sample.drop_mathlock_table(tab_name)
    postgres_sample.create_mathlock_table(tab_name)
    # mathlock_postgres.delete_row(tab_name, record_index)
    encrypted1 = postgres_sample.rest_do_encryption(value1)
    encrypted2 = postgres_sample.rest_do_encryption(value2)

    st1 = postgres_sample.build_string_for_postgres(encrypted1)
    st2 = postgres_sample.build_string_for_postgres(encrypted2)
    postgres_sample.insert_into_table(tab_name, record_index, st1, st2)
    postgres_sample.div_matrices(tab_name, record_index)
    # get_all = postgres_sample.select_all_from_table(tab_name)

    result = postgres_sample.select_math_result_by_row_id(tab_name, record_index)
    result_to_decrypt = postgres_sample.prepare_data_for_decryption(result)
    print(f"Result ciphertext before decryption: {result_to_decrypt}")

    decrypted = postgres_sample.rest_do_decryption(result_to_decrypt)
    print(f"Final result after decryption is: {decrypted['value']}")
