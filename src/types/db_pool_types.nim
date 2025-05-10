#Type definitions for the database pool and table objects

#Define a type for a table, we are using a reference type here so we can pass the table object around without having to copy it
type TableObj* = ref object 
    pool: PgPool
    name*: string
    columns*: seq[string]
    col_types*: seq[string]

#Define a custom type for our database interface, this will handle running queries via the async query pool and returning responses
type DB* = ref object
    tables: ref Table[string, TableObj]#Define a table to store the table objects in, this is to allow them to be resused as creating the table object requires a db query
    connections: int32#Define the number of threads to use for the async query pool
    pool: PgPool
    lock: Lock#Define a lock that will be used to ensure only 1 thread can alter the db object a time (needed for things like .load_table but not needed for .query method)
