from constants import*
from enum import Enum
from library.functions import*



class  StatementType(Enum):
    insert = 1
    select = 2


class PrepareResults(Enum):
    success = 1
    syntax_error = 2
    error = 3
    unrecognized_statement = 4
    negative_id = 5
    string_too_long = 6


class MetaCommandResults(Enum):
    success = 1
    unrecognized = 2


class Execute_result(Enum):
    success = 1
    Table_full = 2


class Statment:
    def __init__(self, type:  StatementType, row_to_insert: row) -> None:
        self.type = type
        self.row_to_insert = row_to_insert



def meta_command(input_string: str) -> MetaCommandResults:
    if input_string == ".exit":
        return MetaCommandResults.success
    else:
        return MetaCommandResults.unrecognized


def preparestatement(input_string: str) -> tuple[Statment, PrepareResults]:
    str_split = input_string.split()
    statement_type = str_split[0]
    match statement_type:
        case "insert":
            if len(str_split) < 4:
                return (None, PrepareResults.syntax_error)
            type = StatementType.insert
            id = int(str_split[1])
            username = str_split[2]
            email = str_split[3]
            row_to_insert = row(id, username, email)
            statement = Statment(type, row_to_insert)
            if id < 0:
                return (statement, PrepareResults.negative_id)
            if len(username) > USERNAME_SIZE or len(email) > EMAIL_SIZE:
                return (statement, PrepareResults.string_too_long)
            return (statement, PrepareResults.success)
        case "select":
            type = StatementType.select
            statement = Statment(type, None)
            return (statement, PrepareResults.success)
        case _:
            return (None, PrepareResults.error)


def execute_insert(statement: Statment, table: table) -> Execute_result:
    if table.num_rows >= TABLE_MAX_ROWS:
        return Execute_result.Table_full
    row_to_insert = statement.row_to_insert
    cursor=table_end(table)
    serialized_row = serialize_row(row_to_insert)
    slot = table.cursor_value(cursor)
    slot[: len(serialized_row)] = serialized_row
    table.num_rows += 1
    del cursor
    return Execute_result.success


def execute_select(statement: Statment, table: table) -> Execute_result:
    cursor=table_start(table)
    while(cursor.end_of_table is not True):
        data = table.cursor_value(cursor)
        row = deserialize_row(data)
        print_row(row)
        cursor_advance(cursor)
    del cursor
    return Execute_result.success


def Executestatement(statement: Statment, table: table) -> None:
    match statement.type: 
        case StatementType.insert:
            return execute_insert(statement, table)
        case StatementType.select:
            return execute_select(statement, table)


def do_meta_command(input_string: str, table: table) -> MetaCommandResults:
    if input_string == ".exit":
        db_close(table)
        print("")
        exit(0)
    else:
        return MetaCommandResults.unrecognized