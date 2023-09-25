from library.statements import*
import struct
from typing import Tuple
from constants import*





def new_table() -> table:
    return table()


def free_table(Table: table) -> None:
    for page in Table.pages:
        if page is not None:
            page.clear()
    Table.pages = []


def repl() -> None:
    table = db_open("db")
    while True:
        # try:
            userInput = input("sqlite  ")
            if userInput[0] == ".":
                result = meta_command(userInput)
                match result:
                    case MetaCommandResults.success:
                        do_meta_command(userInput,table)
                        continue
                    case MetaCommandResults.unrecognized:
                        print(f"unrecognized Command {userInput}")
                        continue
            else:
                statement, result = preparestatement(userInput)
                match result:
                    case PrepareResults.success:
                        execution_result = Executestatement(statement, table)
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





if __name__ == "__main__":
    repl()
