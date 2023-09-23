from enum import Enum

from constants import *
from lib.database import *


class StatementType(Enum):
    INSERT = 1
    SELECT = 2

class Statement:
    def __init__(self, type: StatementType, row_to_insert: Row):
        self.type = type
        self.row_to_insert = row_to_insert


class PrepareResult(Enum):
    SUCCESS = 1
    SYNTAX_ERROR = 2
    ERROR = 3
    UNRECOGNIZED_STATEMENT = 4
    NEGATIVE_ID = 5
    STRING_TOO_LONG = 6


class MetaCommandResult(Enum):
    SUCCESS = 1
    UNRECOGNIZED = 2


class ExecuteResult(Enum):
    SUCCESS = 1
    TABLE_FULL = 2

def prepare_statement(input_string: str) -> (Statement or None, PrepareResult):
    str_split = input_string.split()
    statement_type = str_split[0]
    match statement_type:
        case "insert":
            if len(str_split) < 4:
                return (None, PrepareResult.SYNTAX_ERROR)
            type = StatementType.INSERT
            id = int(str_split[1])
            username = str_split[2]
            email = str_split[3]
            row = Row(id, username, email)
            statement = Statement(type, row)
            if id < 0:
                return (statement, PrepareResult.SUCCESS)
            if len(username) > USERNAME_SIZE:
                return (statement, PrepareResult.STRING_TOO_LONG)
            if len(email) > EMAIL_SIZE:
                return (statement, PrepareResult.STRING_TOO_LONG)
            return (statement, PrepareResult.SUCCESS)
        case "select":
            type = StatementType.SELECT
            statement = Statement(type, None)
            return (statement, PrepareResult.SUCCESS)
        case _:
            return (None, PrepareResult.UNRECOGNIZED_STATEMENT)


def execute_insert(statement: Statement, table: Table) -> ExecuteResult:
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.TABLE_FULL
    row_to_insert = statement.row_to_insert
    serialized_row = serialize_row(row_to_insert)
    slot = table.row_slot(table.num_rows)
    slot[: len(serialized_row)] = serialized_row
    table.num_rows += 1
    return ExecuteResult.SUCCESS

def execute_select(statement: Statement, table: Table) -> ExecuteResult:
    for i in range(table.num_rows):
        data = table.row_slot(i)
        row = deserialize_row(data)
        print_row(row)
    return ExecuteResult.SUCCESS


def execute_statement(statement: Statement, table: Table) -> ExecuteResult:
    match statement.type:
        case StatementType.INSERT:
            return execute_insert(statement, table)
        case StatementType.SELECT:
            return execute_select(statement, table)


def do_meta_command(input_string: str, table: Table) -> MetaCommandResult:
    if input_string == ".exit":
        free_table(table)
        exit(0)
    else:
        return MetaCommandResult.UNRECOGNIZED