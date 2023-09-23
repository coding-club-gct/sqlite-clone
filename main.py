from enum import Enum
import struct
from typing import Tuple

ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

# Define the Table structure
PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES


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


class row:
    def __init__(self, id: int, username: str, email: str) -> None:
        self.id = id
        self.username = username
        self.email = email


class table:
    def __init__(self) -> None:
        self.num_rows = 0
        self.pages = [None] * TABLE_MAX_PAGES

    def row_slot(self, row_num):
        page_num = row_num // ROWS_PER_PAGE
        page = self.pages[page_num]
        if page is None:
            page = self.pages[page_num] = bytearray(PAGE_SIZE)
        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        return memoryview(page)[byte_offset:byte_offset + ROW_SIZE]


class Statment:
    def __init__(self, type:  StatementType, row_to_insert: row) -> None:
        self.type = type
        self.row_to_insert = row_to_insert


def serialize_row(Row: row):
    return struct.pack(
        f"{ID_SIZE}s{USERNAME_SIZE}s{EMAIL_SIZE}s",
        Row.id.to_bytes(ID_SIZE, byteorder="little"),
        Row.username.encode("utf-8").ljust(USERNAME_SIZE, b"\0"),
        Row.email.encode("utf-8").ljust(EMAIL_SIZE, b"\0"),
    )


def deserialize_row(data) -> row:
    try:
        unpacked_data = struct.unpack(
            f"{ID_SIZE}s{USERNAME_SIZE}s{EMAIL_SIZE}s", data
        )
        return row(
            int.from_bytes(unpacked_data[0], byteorder="little"),
            unpacked_data[1].decode("utf-8").rstrip("\x00"),
            unpacked_data[2].decode("utf-8").rstrip("\x00"),
        )
    except struct.error:
        return None


def print_row(Row: row) -> None:
    print(Row.id, Row.username, Row.email)


def new_table() -> table:
    return table()


def free_table(Table: table) -> None:
    for page in Table.pages:
        if page is not None:
            page.clear()
    Table.pages = []


def repl() -> None:
    Table = new_table()
    while True:
        # try:
            userInput = input("sqlite  ")
            if userInput[0] == ".":
                result = meta_command(userInput)
                match result:
                    case MetaCommandResults.success:
                        do_meta_command(userInput,Table)
                        continue
                    case MetaCommandResults.unrecognized:
                        print(f"unrecognized Command {userInput}")
                        continue
            else:
                statement, result = preparestatement(userInput)
                match result:
                    case PrepareResults.success:
                        execution_result = Executestatement(statement, Table)
                        match execution_result:
                            case Execute_result.success:
                                print("Executed..")
                            case Execute_result.Table_full:
                                print("Error: Table Full")
                                break
                    case PrepareResults.ERROR:
                        print(f"Unrecognized keyword at the start of {userInput}")
                        continue
                    case PrepareResults.NEGATIVE_ID:
                        print("ID must be positive")
                        continue
                    case PrepareResults.STRING_TOO_LONG:
                        print("String is too long")
                        continue
                    case PrepareResults.syntax_error:
                        print("Syntax error. Could not parse statement.")
                        continue
                    case PrepareResults.error:
                        print(f"Unrecognized error {userInput}")
                        continue

    #    except Exception as e:
    #          print(f"Error: {e}")


def meta_command(input_string: str) -> MetaCommandResults:
    if input_string == ".exit":
        return MetaCommandResults.success
    else:
        return MetaCommandResults.unrecognized


def preparestatement(input_string: str) -> Tuple[Statment, PrepareResults]:
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
    serialized_row = serialize_row(row_to_insert)
    slot = table.row_slot(table.num_rows)
    slot[: len(serialized_row)] = serialized_row
    table.num_rows += 1
    return Execute_result.success


def execute_select(statement: Statment, table: table) -> Execute_result:
    for i in range(table.num_rows):
        data = table.row_slot(i)
        row = deserialize_row(data)
        print_row(row)
    return Execute_result.success


def Executestatement(statement: Statment, table: table) -> None:
    match statement.type: 
        case StatementType.insert:
            return execute_insert(statement, table)
        case StatementType.select:
            return execute_select(statement, table)


def do_meta_command(input_string: str, table: table) -> MetaCommandResults:
    if input_string == ".exit":
        free_table(table)
        exit(0)
    else:
        return MetaCommandResults.unrecognized


if __name__ == "__main__":
    repl()
