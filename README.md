# Mysql-stream-write-json

Use node streams to write a newline delimited (\n) .json file to a mysql database

This module uses the node [mysql](https://github.com/mysqljs/mysql) client to write to an instance

Notes: Some issues have been encountered when using node 11+ to manipulate streams. Node 10.x is recommended.

## Usage: 

Within the write-stream.js file, insert the name of the JSON file to be used in the large-table variable:

```sh
const largeTable = '<name of input file>.json';
```

The target table variable holds the title of the table to be created:

```sh
const targetTable = "DATA_TEST";
```

Ensure the database connection options are aligned with database credentials:
```sh
{ 
 ...
  host: "localhost",
  user: "mock_user",
  database: "my_db",
  password: "mock_user's password"
...
}  
```

With all the information above entered correctly, run the program via node (10.x) 

```sh
$ node write-stream.js 
```

The program will automatically print the current line being written. 
The debugger will occasionally print the memory being used by the program.

TODO: 
Batch write multiple lines for efficiency
Write subsequent program to update a table instead of creating a new one every run event.