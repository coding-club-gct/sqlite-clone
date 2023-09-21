from enum import Enum


class Statement(Enum):
    INSERT = 1
    SELECT = 2


class PrepareResult(Enum):
    SUCCESS = 1
    ERROR = 2


class MetaCommandResut(Enum):
    SUCCESS = 1
    UNRECOGNIZED = 2


def repl() -> None:
    while True:
        try:
            user_input = input("sqlite  ")
            if (user_input[0] == '.'):
                match do_meta_command(user_input):
                    case MetaCommandResut.SUCCESS:
                        continue
                    case MetaCommandResut.UNRECOGNIZED:
                        print(r"Unrecognized command {user_input}")
                        continue
            else:
                prepared_statement = prepare_statement(user_input)
                statement = prepared_statement[0]
                prepare_result = prepared_statement[1]
                match prepare_result:
                    case PrepareResult.SUCCESS:
                        execute_statement(statement)
                    case PrepareResult.ERROR:
                        print(r"Unrecognized keyword at start of {user_input}")
                        continue

        except Exception as e:
            print(f"Error: {e}")


def prepare_statement(input_string: str) -> (Statement, PrepareResult):
    str_split = input_string[:6]
    match str_split:
        case "insert":
            return (Statement.INSERT, PrepareResult.SUCCESS)
        case "select":
            return (Statement.SELECT, PrepareResult.SUCCESS)
        case _:
            return (None, PrepareResult.ERROR)


def execute_statement(statement: Statement) -> None:
    match statement:
        case Statement.INSERT:
            print("insert statement")
        case Statement.SELECT:
            print("select statement")


def do_meta_command(input_string: str) -> MetaCommandResut:
    if (input_string == ".exit"):
        return MetaCommandResut.SUCCESS
    else:
        return MetaCommandResut.UNRECOGNIZED


if __name__ == "__main__":
    repl()
