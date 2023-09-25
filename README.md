---

# Simple Python Database Management System

The motivation of this project is to learn Python, operating system concepts (paging), and data structures (B-tree). The inspiration for this project comes from a C tutorial page, which I have studied and implemented in Python. While the original tutorial was in C, I have successfully understood the concepts and adapted them to Python.

This is a simple command-line database management system implemented in Python. It allows you to create and manage a basic database with support for inserting and selecting rows. Data persisting is achieved through file-based storage.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [Contributing](#contributing)
- [License](#license)
- [To-Do](#to-do)

## Features


- *Motivation:* This project is motivated by the desire to learn Python, operating system concepts related to paging, and data structures, particularly B-trees.


- *Basic Database Operations:* This database system supports two basic SQL operations: `INSERT` to add rows to the database and `SELECT` to retrieve rows.

- *Pager and Cursor:* The code includes a `Pager` class for file management and a `Cursor` class for navigating rows in the table.

- *Data Serialization:* Rows are serialized into binary format for storage and deserialized for retrieval.

- *Data Persisting:* The database is designed to persist data through file-based storage.


- *Error Handling:* The code handles errors such as file I/O issues and invalid SQL statements.

## Installation

1. Clone this repository to your local machine:

   
   git clone https://github.com/yourusername/your-repo-name.git
   

2. Navigate to the project directory:

   
   cd your-repo-name
   

3. Run the Python script:

   
   python main.py
   

## Usage

1. *Inserting Rows:*

   Use the `INSERT` SQL statement to add rows to the table. For example:

   
   Insert 1 Alice alice@example.com
   

2. *Selecting Rows:*

   Use the `SELECT` SQL statement to retrieve rows from the table. For example:

   
   select
   

3. *Exiting the Database:*

   To exit the database, use the `.exit` meta-command:

   
   .exit
   

## File Structure

- `main.py`: The main Python script for the database management system.
- `constants.py`: Constants and configurations for the database.
- `lib/`: Directory containing database-related classes and functions.
- `README.md`: This file, providing information about the project.

## Contributing

Contributions are welcome! If you'd like to contribute to this project, please follow these guidelines:

1. Fork the repository.

2. Create a new branch for your feature or bug fix.

3. Make your changes and commit them with clear, concise commit messages.

4. Push your changes to your fork.

5. Submit a pull request to the original repository.


## To-Do

- [ ] Implement B-tree data structure for more efficient data storage and retrieval.

---

This README file provides project-specific details, usage examples, and additional information to help other users understand and use your code effectively.