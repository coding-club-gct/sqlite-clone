from constants import *
from lib.database import *
from lib.statement import *


def repl() -> None:
    table = new_table()
    while True:
        try:
            user_input = input("sqlite  ")
            if user_input[0] == ".":
                match do_meta_command(user_input, table):
                    case MetaCommandResult.SUCCESS:
                        continue
                    case MetaCommandResult.UNRECOGNIZED:
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
                                print("Error: Table full.")
                                break
                    case PrepareResult.ERROR:
                        print(f"Unrecognized keyword at start of {user_input}")
                        continue
                    case PrepareResult.NEGATIVE_ID:
                        print("ID must be positive")
                        continue
                    case PrepareResult.STRING_TOO_LONG:
                        print("String is too long")
                        continue
                    case PrepareResult.SYNTAX_ERROR:
                        print("Syntax error. Could not parse statement.")
                        continue

        except Exception as e:
            print(f"Error: {e}")





if __name__ == "__main__":
    repl()
