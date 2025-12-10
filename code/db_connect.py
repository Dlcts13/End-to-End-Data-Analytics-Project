try:
    import sqlalchemy
except ModuleNotFoundError:
    sqlalchemy = None
    print("Warning: module 'sqlalchemy' not installed")

try:
    import pyodbc
except ModuleNotFoundError:
    pyodbc = None
    print("Warning: module 'pyodbc' not installed.")


def get_db_connection():
    server = r"localhost\SQLEXPRESS"
    database = "customer_behavior"

    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        'Trusted_Connection=yes;'
    )

    if pyodbc is None:
        print("pyodbc is not available — cannot connect to database.")
        return None

    try:
        conn = pyodbc.connect(connection_string)
        print("Successfully connected to the database!")
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        return None


if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        conn.close()