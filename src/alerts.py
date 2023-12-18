from enum import Enum


class AlertType(Enum):
    """
    Enum class for message box prompts
    """

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Messages(Enum):
    """
    Enum class for console prompt messages
    """

    SQL_Q_ERROR = "An SQL SELECT statement error occurred"
    SUCCESS_LOAD_OP = "The INSERT operation completed successfully"
    SQL_I_ERROR = "There was an error in the INSERT query"
    DB_CONN_ERROR = "A database connection error occurred"
    RUN_T_ERROR = "A runtime error occurred"
    ASYNC_ERROR = "An asynchronous runtime error occurred"
    PROGRAM_END = "End of program execution"
    SUCCESS_OP = "The operation completed successfully"
    SUCCESS_UPDATE_OP = "The UPDATE operation completed successfully"
    SQL_U_ERROR = "There was an error in the UPDATE query"
    SQL_D_ERROR = "There was an error in the DELETE query"
    GENERIC_ERROR = "An error occurred and the operation did not complete. Please check log."
