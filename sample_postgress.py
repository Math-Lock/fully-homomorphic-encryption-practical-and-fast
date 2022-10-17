import sys
import arrow
import psycopg2
import sample_mathlock_rest as rest_sample
import pandas as pd
from typing import Generator

pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.options.mode.chained_assignment = None


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
                 port="54141", pandas_cell_len: [int, None] = None) -> None:
        self.conn = psycopg2.connect(database=database, host=host, user=user, password=password, port=port)
        self.cursor = self.conn.cursor()
        self.rest_sample = rest_sample.SampleMathLockApp()  # takes another sample instance
        self.mult_result = "mult_result"  # column name for FHE multiplication results
        self.div_result = "div_result"  # column name for FHE division results
        self.add_result = "add_result"  # column name for FHE addition results
        self.sub_result = "sub_result"  # column name for FHE subtraction results
        self.number1 = "number1"
        self.number2 = "number2"
        pd.set_option('display.max_colwidth', pandas_cell_len)  # None gives unlimited length

    # region public methods

    def create_extension(self, ext_name: str = "mathlock") -> None:
        """ The method creates custom, proprietary Postgres extension writen by Math-Lock. On our demo-server it's
        already compiled and created, so in fact this method here just for the sake of stability. """
        cmd = f"CREATE EXTENSION IF NOT EXISTS {ext_name}"
        self.cursor.execute(cmd)
        self.conn.commit()

    def get_all_table_names(self) -> Generator:
        """ The method gives all public table names available in the db """
        self.cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema "
                            f"NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE' ORDER BY "
                            f"table_name;")
        tables = self.cursor.fetchall()
        for table in tables:
            yield table[0]

    def get_all_column_names(self, table_name) -> list:
        """ The method gives all column names in the given table """
        self.cursor.execute(f"SELECT * FROM {table_name}")
        column_names = [desc[0] for desc in self.cursor.description]
        for i in column_names:
            print(i)
        return column_names

    def print_entire_table(self, table_name):
        """ D """
        my_table = pd.read_sql(f"SELECT * FROM {table_name}", self.conn)
        # another_attempt = psql.read_sql(f"SELECT * FROM {table_name}", self.conn)
        print(my_table)
        # print(another_attempt)

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

    def select_math_result_by_row_id(self, table_name: str, row_id: int, column_name: str) -> dict:
        """ The method performs select by a record ID. Generally we store math. operations result into column N3 """
        self.cursor.execute(f"SELECT {column_name} FROM {table_name} WHERE id = {row_id}")
        res = self.cursor.fetchall()
        print(f"Fetched result by ID: {row_id}, from table: [{table_name}], after math operation: {column_name},"
              f"ciphertext is: {res}")

        return self.prepare_data_for_decryption(res)

    def is_table_exists(self, table_name: str) -> bool:
        """ The method validates whether table exists or not """
        cmd = f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'"
        self.cursor.execute(cmd)
        info = self.cursor.fetchone()
        if info:
            return True

        return False

    def create_mathlock_table(self, table_name: str) -> bool:
        """ The method creates a table by given name """
        if not table_name:
            print(f"Table name is empty, terminating")
            sys.exit(1)

        if not self.is_table_exists(table_name):
            cmd = f"CREATE TABLE IF NOT EXISTS public.{table_name} (id integer NOT NULL, " \
                  f"{self.number1} public.mathlock, {self.number2} public.mathlock, {self.mult_result} public.mathlock," \
                  f"{self.div_result} public.mathlock, {self.add_result} public.mathlock, " \
                  f"{self.sub_result} public.mathlock);"
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
        cmd = f"INSERT INTO public.{table_name} (id, {self.number1}, {self.number2}) VALUES ({index}, '{m1}', '{m2}');"
        self.cursor.execute(cmd)
        self.conn.commit()

    def drop_mathlock_table(self, table_name: str) -> bool:
        """ The method drops existing table by given name """
        if not table_name:
            print(f"\nTable name is empty, terminating")
            sys.exit(1)

        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.commit()
        if not self.is_table_exists(table_name):
            print(f"\nTable: [{table_name}] has been deleted successfully")
            return True

        print(f"\nTable: [{table_name}] can't be deleted, or doesn't exists")
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

    def homomorphic_multiplication(self, table_name: str, row_id: int) -> None:
        """ The method performs FHE matrix multiplication inside Postgres by our custom extension, using given table name
         and row (record) ID """
        print(f"Called FHE Multiplication operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET {self.mult_result} = {self.number1} * {self.number2} WHERE id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def homomorphic_division(self, table_name, row_id) -> None:
        """ The method performs FHE matrix division inside Postgres by our custom extension, using given table name
        and row (record) ID """
        print(f"Called FHE Division operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET {self.div_result} = {self.number1} / {self.number2} WHERE id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def homomorphic_addition(self, table_name, row_id) -> None:
        """ The method performs FHE matrix addition inside Postgres by our custom extension, using given table name
         and row (record) ID """
        print(f"Called FHE Addition operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET {self.add_result} = {self.number1} + {self.number2} WHERE id = {row_id};"
        self.cursor.execute(cmd)
        self.conn.commit()

    def homomorphic_subtraction(self, table_name, row_id) -> None:
        """ The method performs FHE matrix subtraction inside Postgres by our custom extension, using given table name
         and row (record) ID """
        print(f"Called FHE Subtraction operation for the table: {table_name} row ID {row_id}")
        cmd = f"UPDATE public.{table_name} SET {self.sub_result} = {self.number1} - {self.number2} WHERE id = {row_id};"
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

    def rest_do_decryption(self, m1: dict, ops_type: str) -> dict:
        """ The method performs Fully Homomorphic Decryption operation using our other sample for REST API """
        res = self.rest_sample.do_decryption(m1)
        print(f"Result after decryption for {ops_type} is: {res['value']}")
        return res

    def build_string_for_postgres(self, value: dict) -> str:
        """ String builder to match to some specific syntax """
        return f"{'{'}{value['a']},{value['b']},{value['c']},{value['d']}{'}'}"

    def prepare_data_for_decryption(self, data: dict) -> dict:
        """ The method prepares data for decryption via REST API """
        rebuild_string = str(data[0][0]).replace('{', '').replace('}', '').split(",", 4)
        return {"a": rebuild_string[0], "b": rebuild_string[1], "c": rebuild_string[2], "d": rebuild_string[3]}

    # endregion


def run():
    """ Runs entire execution """

    set_values1 = ['10.5', '2.3', '347873.1289', '1']
    set_values2 = ['5.34', '-99.9', '344.9', '1']
    tab_name = "test_table"  # please make sure to define unique name of your table
    indexes = range(1, len(set_values1) + 1)
    postgres_sample = MathLockPostgresSample(pandas_cell_len=30)  # define by param 'pandas_row_len' pandas cell length
    postgres_sample.create_extension()
    print(f"Original values to operate with are: {set_values1} and {set_values2}")
    # delete table in case it was created before, or was some mistake there. Comment it out if not needed
    postgres_sample.drop_mathlock_table(tab_name)
    postgres_sample.create_mathlock_table(tab_name)

    # By default, we create only records with indexes for the above set of values, every time deleting the table.
    # If you want to avoid deletion or extend it anyhow - feel free.
    # and to use multiple indexes - comment out deletion line and change the code accordingly
    for num1, num2, rec_index in zip(set_values1, set_values2, indexes):
        encrypted1 = postgres_sample.rest_do_encryption(num1)
        encrypted2 = postgres_sample.rest_do_encryption(num2)

        st1 = postgres_sample.build_string_for_postgres(encrypted1)
        st2 = postgres_sample.build_string_for_postgres(encrypted2)
        postgres_sample.insert_into_table(tab_name, rec_index, st1, st2)

        # perform all 4 homomorphic operations and add into the table for relative columns
        postgres_sample.homomorphic_division(tab_name, rec_index)
        postgres_sample.homomorphic_multiplication(tab_name, rec_index)
        postgres_sample.homomorphic_addition(tab_name, rec_index)
        postgres_sample.homomorphic_subtraction(tab_name, rec_index)

        # fetch homomorphic results and decrypt them
        mult_result = postgres_sample.select_math_result_by_row_id(tab_name, rec_index, postgres_sample.mult_result)
        add_result = postgres_sample.select_math_result_by_row_id(tab_name, rec_index, postgres_sample.add_result)
        div_result = postgres_sample.select_math_result_by_row_id(tab_name, rec_index, postgres_sample.div_result)
        sub_result = postgres_sample.select_math_result_by_row_id(tab_name, rec_index, postgres_sample.sub_result)

        postgres_sample.rest_do_decryption(mult_result, "multiplication")
        postgres_sample.rest_do_decryption(div_result, "division")
        postgres_sample.rest_do_decryption(add_result, "addition")
        postgres_sample.rest_do_decryption(sub_result, "subtraction")

    print("\n Our encrypted table view: ")
    postgres_sample.print_entire_table(tab_name)


if __name__ == "__main__":
    run()
