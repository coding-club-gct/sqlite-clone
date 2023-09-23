import struct
from constants import *


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
        return memoryview(page)[byte_offset : byte_offset + ROW_SIZE]
    
class Row:
    def __init__(self, id: int, username: str, email: str):
        self.id = id
        self.username = username
        self.email = email


def new_table() -> Table:
    return Table()


def free_table(table: Table) -> None:
    for page in table.pages:
        if page is not None:
            page.clear()
    table.pages = []

def serialize_row(row: Row):
    return struct.pack(
        f"{ID_SIZE}s{USERNAME_SIZE}s{EMAIL_SIZE}s",
        row.id.to_bytes(ID_SIZE, byteorder="little"),
        row.username.encode("utf-8").ljust(USERNAME_SIZE, b"\0"),
        row.email.encode("utf-8").ljust(EMAIL_SIZE, b"\0"),
    )


def deserialize_row(data) -> Row:
    try:
        unpacked_data = struct.unpack(f"{ID_SIZE}s{USERNAME_SIZE}s{EMAIL_SIZE}s", data)
        return Row(
            int.from_bytes(unpacked_data[0], byteorder="little"),
            unpacked_data[1].decode("utf-8").rstrip("\x00"),
            unpacked_data[2].decode("utf-8").rstrip("\x00"),
        )
    except struct.error:
        return None


def print_row(row: Row) -> None:
    print(row.id, row.username, row.email)