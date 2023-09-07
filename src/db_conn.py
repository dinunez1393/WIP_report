# Functions for database connection
import pyodbc
import aioodbc


def make_connection(server_name, database_name):
    """
    Function to make the connection to SQL database
    :param server_name: The SQL server name
    :type server_name: str
    :param database_name: The Database name from the SQL server
    :type database_name: str
    :return: The connection made to the database
    :rtype: pyodbc.Connection
    """
    dsn = f"""Driver={{SQL Server}}; 
    Server={server_name}; 
    Database{database_name}; 
    Trusted_Connection=yes;"""
    connection = pyodbc.connect(dsn)
    return connection


async def async_db_connection(server_name, database_name):
    """
    Function that makes ASYNC connection to SQL database
    :param server_name: The SQL server name
    :type server_name: str
    :param database_name: The Database name from the SQL server
    :type database_name: str
    :return: The async connection made to the database
    """
    dsn = f"""
        Driver={{SQL Server}};
        Server={server_name};
        Database={database_name};
        Trusted_Connection=yes;
    """

    async_connection = await aioodbc.connect(dsn=dsn)
    return async_connection


async def create_async_pool(server_name, database_name):
    """
    Function that creates an ASYNC Pool for various (maxsize) simultaneous SELECT queries to one table
    :param server_name: The SQL server name
    :type server_name: str
    :param database_name: The Database name from the SQL server
    :type database_name: str
    :return: The asynchronous pool for the database
    """
    dsn = f"""
            Driver={{SQL Server}};
            Server={server_name};
            Database={database_name};
            Trusted_Connection=yes;
        """
    pool = await aioodbc.create_pool(dsn=dsn, minsize=1, maxsize=15)
    return pool
