from enum import Enum
import struct

# Define the compact representation of a row
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


class StatementType(Enum):
    INSERT = 1
    SELECT = 2


class PrepareResult(Enum):
    SUCCESS = 1
    SYNTAX_ERROR = 2
    UNRECOGNIZED_STATEMENT = 2


class MetaCommandResut(Enum):
    SUCCESS = 1
    UNRECOGNIZED = 2

class ExecuteResult(Enum):
    SUCCESS = 1
    TABLE_FULL = 2


class Row:
    def __init__(self, id: int, username: str, email: str):
        self.id = id
        self.username = username
        self.email = email

class Table:
    def __init__(self) -> None:
        self.num_rows = 0
        self.pages = [None] * TABLE_MAX_PAGES
    
    def row_slot(self, row_num):
        page_num = row_num // ROWS_PER_PAGE
        page = self.pages[page_num]
        if page is None:
            # Allocate memory only when we try to access the page
            page = self.pages[page_num] = bytearray(PAGE_SIZE)
        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        return memoryview(page)[byte_offset:byte_offset + ROW_SIZE]
    

class Statement:
    def __init__(self, type: StatementType, row_to_insert: Row):
        self.type = type
        self.row_to_insert = row_to_insert


def serialize_row(row: Row):
    return struct.pack(f"{ID_SIZE}s{USERNAME_SIZE}s{EMAIL_SIZE}s",
                       row.id.to_bytes(ID_SIZE, byteorder='little'),
                       row.username.encode('utf-8').ljust(USERNAME_SIZE, b'\0'),
                       row.email.encode('utf-8').ljust(EMAIL_SIZE, b'\0'))



def deserialize_row(data) -> Row:
    try:
        unpacked_data = struct.unpack(f"{ID_SIZE}s{USERNAME_SIZE}s{EMAIL_SIZE}s", data)
        return Row(int.from_bytes(unpacked_data[0], byteorder='little'),
                   unpacked_data[1].decode('utf-8').rstrip('\x00'),
                   unpacked_data[2].decode('utf-8').rstrip('\x00'))
    except struct.error:
        return None



def print_row(row: Row) -> None:
    print(row.id, row.username, row.email)


def new_table() -> Table:
    return Table()


def free_table(table: Table) -> None:
    for page in table.pages:
        if page is not None:
            page.clear()
    table.pages = []


def repl() -> None:
    table = new_table()
    while True:
        try:
            user_input = input("sqlite  ")
            if (user_input[0] == '.'):
                match do_meta_command(user_input, table):
                    case MetaCommandResut.SUCCESS:
                        continue
                    case MetaCommandResut.UNRECOGNIZED:
                        print(f"Unrecognized command {user_input}")
                        continue
            else:
                prepared_statement = prepare_statement(user_input)
                statement = prepared_statement[0]
                prepare_result = prepared_statement[1]
                match prepare_result:
                    case PrepareResult.SUCCESS:
                        match execute_statement(statement, table):
                            case ExecuteResult.SUCCESS:
                                print("Executed.")
                            case ExecuteResult.TABLE_FULL:
                                print("ErrorL Table full.")
                                break
                    case PrepareResult.ERROR:
                        print(f"Unrecognized keyword at start of {user_input}")
                        continue

        except Exception as e:
            print(f"Error: {e}")


def prepare_statement(input_string: str) -> (Statement, PrepareResult):
    str_split = input_string.split()
    statement_type = str_split[0]
    match statement_type:
        case "insert":
            if len(str_split) < 4:
                return (None, PrepareResult.SYNTAX_ERROR)
            type = StatementType.INSERT
            row = Row(int(str_split[1]), str_split[2], str_split[3])
            statement = Statement(type, row)
            return (statement, PrepareResult.SUCCESS)
        case "select":
            type = StatementType.SELECT
            statement = Statement(type, None)
            return (statement, PrepareResult.SUCCESS)
        case _:
            return (statement, PrepareResult.UNRECOGNIZED_STATEMENT)


def execute_insert(statement: Statement, table: Table) -> ExecuteResult:
    if(table.num_rows >= TABLE_MAX_ROWS):
        return ExecuteResult.TABLE_FULL
    row_to_insert = statement.row_to_insert
    serialized_row = serialize_row(row_to_insert)
    slot = table.row_slot(table.num_rows)
    slot[:len(serialized_row)] = serialized_row
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


def do_meta_command(input_string: str, table: Table) -> MetaCommandResut:
    if (input_string == ".exit"):
        free_table(table)
        exit(0)
    else:
        return MetaCommandResut.UNRECOGNIZED


if __name__ == "__main__":
    repl()
