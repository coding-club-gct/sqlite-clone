import errno
import os
import struct
from constants import *


class Pager:
    def __init__(self) -> None:
        self.file_descriptor = 0
        self.file_length = 0
        self.pages = [None] * TABLE_MAX_PAGES


class Table:
    def __init__(self) -> None:
        self.num_rows = 0
        self.pager: Pager or None = None

    def row_slot(self, row_num):
        page_num = row_num // ROWS_PER_PAGE
        page = get_page(self.pager, page_num)
        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        return memoryview(page)[byte_offset : byte_offset + ROW_SIZE]


class Row:
    def __init__(self, id: int, username: str, email: str):
        self.id = id
        self.username = username
        self.email = email


def db_open(filename: str) -> Table:
    pager = pager_open(filename)
    num_rows = pager.file_length // ROW_SIZE
    table = Table()
    table.num_rows = num_rows
    table.pager = pager
    return table


def pager_open(filename: str) -> Pager:
    try:
        file_descriptor = os.open(
            filename,
            os.O_RDWR | os.O_CREAT,
            0o600,
        )
        file_length = os.path.getsize(filename)
        pager = Pager()
        pager.filename = filename
        pager.file_descriptor = file_descriptor
        pager.file_length = file_length
        return pager

    except Exception as e:
        print(f"Unable to open file: {str(e)}")
        exit(1)


def get_page(pager: Pager, page_num: int):
    if page_num >= TABLE_MAX_PAGES:
        print(
            f"Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}"
        )
        exit(1)
    if pager.pages[page_num] is None:
        # Cache miss. Allocate memory and load from file.
        page = bytearray(PAGE_SIZE)
        num_pages = pager.file_length // PAGE_SIZE
        # We might save a partial page at the end of the file
        if pager.file_length % PAGE_SIZE:
            num_pages += 1

        if page_num <= num_pages:
            os.lseek(pager.file_descriptor, page_num * PAGE_SIZE, os.SEEK_SET)
            bytes_read = os.read(pager.file_descriptor, PAGE_SIZE)
            if bytes_read == -1:
                print(f"Error reading file: {os.strerror(errno.errno)}")
                exit(1)

        page[:len(bytes_read)] = bytes_read
        pager.pages[page_num] = page

    return pager.pages[page_num]


def db_close(table: Table) -> None:
    pager = table.pager
    num_full_pages = table.num_rows // ROWS_PER_PAGE

    for i in range(num_full_pages):
        if pager.pages[i] is not None:
            pager_flush(pager, i, PAGE_SIZE)
            pager.pages[i] = None

    num_additional_rows = table.num_rows % ROWS_PER_PAGE
    if num_additional_rows > 0:
        page_num = num_full_pages
        if pager.pages[page_num] is not None:
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE)
            pager.pages[page_num] = None

    os.close(pager.file_descriptor)
    for i in range(TABLE_MAX_PAGES):
        page = pager.pages[i]
        if page is not None:
            pager.pages[i] = None

    del pager
    del table


def pager_flush(pager: Pager, page_num: int, size: int) -> None:
    if pager.pages[page_num] is None:
        print("Tried to flush null page")
        exit(1)

    offset = os.lseek(pager.file_descriptor, page_num * PAGE_SIZE, os.SEEK_SET)

    if offset == -1:
        print(f"Error seeking: {os.strerror(errno.errno)}")
        exit(1)

    bytes_written = os.write(pager.file_descriptor, pager.pages[page_num][:size])

    if bytes_written == -1:
        print(f"Error writing: {os.strerror(errno.errno)}")
        exit(1)



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
