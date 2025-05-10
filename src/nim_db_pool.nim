import strutils

#Enable code reordering as its an experimental feature
{.experimental: "codeReordering".}

#Import postgres db
import db_connector/db_postgres

#Import the tables and json modules for handling json type columns
import std/[tables, json, locks, asyncdispatch]

#Include the type definitions for the db pool
include "./types/db_query_types.nim"
include "./types/db_pool_types.nim"

#Now include the async query module, this handles creating 1 or more threads to run queries in the background
include db_async_query

#Define a new constructor for the DB type
proc newDB(creds: ConnCreds, workers: int = 8, reqQueueSize: int = 8192, respQueueSize: int = 8192): DB =
    var db_inst = new(DB)

    #Initilize the tables
    db_inst[].tables = newTable[string, TableObj]()

    #Initilize the postgres pool (req queue size, resp queue size, number of workers)
    var Pool = newPgPool(creds, reqQueueSize.int32, respQueueSize.int32, workers.int32)

    db_inst[].pool = Pool

    #Initilize the lock
    initLock(db_inst.lock)

    db_inst[].connections = 5

    #Now return the db object
    return db_inst

#Methods for DB type
#Define a method that will take a query string and return the resulting rows using the async query pool to run the query
proc query*(self: DB, querystring: string): Future[seq[seq[string]]] {.async} =   
    #Define a results future we will await to obtain the returned rows
    var Result: Future[QueryResult]

    #Run a query and get the returned QueryResult future
    Result = self.pool.pool_query(querystring)

    #Now await the result
    var QueryResultInst = await Result

    #Now return the rows from the QueryResult
    return QueryResultInst.result

#Define a load_table method to create a table object from a table name
proc load_table*(self: DB, name: string): Future[TableObj] {.async.} =
    discard Lock(self.lock)
    

    #First check if the table object already exists in the tables table
    if self.tables.hasKey(name):
        #If it does then return it
        return self.tables[name]

    #If it does not then create a new table object
    else:
        var t: TableObj = new TableObj

        t.name = name#And store the table name

        #Now define sequences for the columns and column types
        t.columns = @[]
        t.col_types = @[]

        #Store the pool object in the table object
        t.pool = self.pool

        #Now build a query to return all column names and there types for the specficied table
        let querystring = "select column_name, udt_name from information_schema.columns where table_name = '" & name & "'"

        #Now run the query and iterate over the returned rows
        let resp = await self.query(querystring)

        #Now iterate over the returned rows and add the column names and types to the table object
        for Row in resp:
            t.columns.add($Row[0])
            t.col_types.add($Row[1])

        #Now add self table object to the key on the db object
        self.tables[name] = t

        #And release the lock before returning
        self.lock.release()

        #Finally return the table object
        return t

#And define a 2nd method that will be used for queries that do not return any rows (update query with no returning clause)
proc queryn*(self: DB, querystring: string): Future[void] {.async.}=
    #Define a results future we will await to obtain the returned rows
    var Result: Future[QueryResult]

    #Run a query and get the returned QueryResult future
    Result = self.pool.pool_query(querystring, true)

    #Now await the result
    discard await Result

#TODO: implement these and return a "transaction" object that can be used to run querys on a specific pool connection and then commit or rollback the transaction
#Needs to handle ensuring no other queries are run on the connection while the transaction is in progress
#Define a begin transaction method
#proc begin*(self: var DB): void =
#    self.conn.exec(sql("begin"))

#Define a commit transaction method
#proc commit*(self: var DB): void =
#    self.conn.exec(sql("commit"))

#Methods for table type
#Define a method to get all rows from a table
proc find*(self: TableObj, where: string = ""): Future[seq[seq[string]]] {.async.} =
    var querystring: string

    if where == "":
        querystring = """select * from """" & self[].name & """""""# """ + The ending " + """ to end the string
    else:
        querystring = """select * from """" & self[].name & """" where """ & where
    
    let Resp = await self.pool.pool_query(querystring)

    return Resp.result 

#Define a method to get a single row from a table
proc find_one*(self: TableObj, where: string = ""): Future[seq[string]] {.async.} =
    var querystring: string

    if where == "":
        querystring = """select * from """" & self.name & """" limit 1"""# """ + The ending " + """ to end the string
    else:
        querystring = """select * from """" & self.name & """" where """ & where & """ limit 1"""
    
    let Resp = await self.pool.pool_query(querystring)

    return Resp.result[0]


#Define a method to count the number of rows in a table that match a where clause
proc count*(self: TableObj, where: string = ""): Future[int] {.async.} =
    var querystring: string

    if where == "":
        querystring = """select count(*) from """" & self.name & """""""# """ + The ending " + """ to end the string
    else:
        querystring = """select count(*) from """" & self.name & """" where """ & where
    
    let Resp = await self.pool.pool_query(querystring)

    return parseInt(Resp.result[0][0])

#Define a method to insert a row into a table, this takes a table of key, value pairs and handles inserting the provided values into the correct columns ommitting any optional/missing columns
proc insert*(self: TableObj, values: Table[string, string]): Future[int] {.async.} =
    #First build a list of the columns we are inserting into (this will be the keys from the values table)
    var cols: seq[string] = @[]
    
    #Also define a list of the values we are inserting (this will be the values from the values table), we are converting these to strings so they can be easily included in the query string
    var insert_vals: seq[string] = @[]

    #Iterate over the keys in the values table and add them to the cols, also lookup there type and add it to the col_types
    for key in values.keys:
        #Add the column name
        cols.add(key)

        #Now lookup the column type
        let index = self.columns.find(key)

        #Check if the column exists in the table
        if index == -1:
            raise newException(ValueError, "Invalid column name: " & key)

        #Now add the value to the values list
        if self.col_types[index] == "varchar" or self.col_types[index] == "json" or self.col_types[index] == "jsonb" or self.col_types[index] == "text":
            insert_vals.add("'" & values[key] & "'")
        else:
            insert_vals.add(values[key])
        
    #Build a string containing the column names that this insert has values for
    var col_string: string
    col_string = """("""" & cols.join("""","""") & """")"""

    #Now build the query string
    var querystring: string
    querystring = """insert into """" & self.name & """" """ & col_string & """ values (""" & insert_vals.join(",") & """) returning id"""

    #Run the query and get the resulting row (will contain the row id)
    let Resp = await self.pool.pool_query(querystring)

    #Now get the result as an integer
    return parseInt(Resp.result[0][0])


#Define a method to insert multiple rows into the table, note that this method assumes all rows have the same columns
#TODO: Update to handle rows with different columns, can do this by setting all missing columns to null
proc insert_many*(self: TableObj, values: seq[Table[string, string]]): Future[seq[int]] {.async.} =
    #First build a list of the columns we are inserting into (this will be the keys from the values table)
    var cols: seq[string] = @[]
    
    #Also define a list of the values we are inserting (this will be the values from the values table), we are converting these to strings so they can be easily included in the query string
    var insert_vals: seq[seq[string]] = @[]

    #Handle adding the columns and there types by iterating over the first row in the values table
    for key in values[0].keys:
        #Add the column name
        cols.add(key)

        #Now lookup the column type
        let index = self.columns.find(key)

        #Check if the column exists in the table
        if index == -1:
            raise newException(ValueError, "Invalid column name: " & key)

    #Iterate over the keys in the values table and add them to the cols, also lookup there type and add it to the col_types
    for row in values:
        #Define a list to hold the values for this row
        var row_vals: seq[string] = @[]

        #Now iterate over entrys on the row and add them to the row vals
        for key in row.keys:
            #Now lookup the column type
            let index = self.columns.find(key)

            #Now add the value to the values list
            if self.col_types[index] == "varchar" or self.col_types[index] == "json" or self.col_types[index] == "jsonb" or self.col_types[index] == "text":
                row_vals.add("'" & row[key] & "'")
            else:
                row_vals.add(row[key])

        #Now add this row to the insert_vals
        insert_vals.add(row_vals)
        
    #Build a string containing the column names that this insert has values for
    var col_string: string
    col_string = """("""" & cols.join("""","""") & """")"""

    #Now assemble the values into strings, each entry will be in the format (val1, val2)
    var insert_vals_string: seq[string] = @[]

    #Iterate over the rows and add them to the insert_vals_string
    for row in insert_vals:
        insert_vals_string.add("(" & row.join(",") & ")")

    #Now build the query string
    var querystring: string
    querystring = """insert into """" & self.name & """" """ & col_string & """ values """ & insert_vals_string.join(",") & """ returning id"""

    #Run the query and get the resulting row (will contain the row id)
    let Resp = await self.pool.pool_query(querystring)

    #Define the sequence of ids to return
    var ids: seq[int] = @[]

    #Now iterate over the rows and add the ids to the ids sequence
    for row in Resp.result:
        ids.add(parseInt(row[0]))

    #Now get the result as an integer
    return ids

#Define a method for updating a row in the table, this takes a table of values to update and a list of columns to match on
proc update*(self: TableObj, values: Table[string, string], uid: seq[string]): Future[int] {.async.} =
    #Define a list of the columns we are inserting into (this will be the keys from the values table) and there types
    var insert_cols: seq[string] = @[] 
    var insert_vals: seq[string] = @[]

    #Now do the same for the filter columns
    var match_cols: seq[string] = @[]
    var match_vals: seq[string] = @[]

    #Iterate over the keys in the values table and add them to the cols, also lookup there type and add it to the col_types
    for key in values.keys:
        #Now lookup the column type
        let index = self.columns.find(key)

        #Check if the column exists in the table
        if index == -1:
            raise newException(ValueError, "Invalid column name: " & key)            

        #Now check if this columns is one of the columns we are matching on
        if uid.find(key) == -1:#Column is not in the where clause            
            #Add the column name
            insert_cols.add(key)

            #Now its value
            #If the column is a string then add it to the match_cols in ''
            if self.col_types[index] == "varchar":
                insert_vals.add(""" """" & key & """" = """ & "'" & values[key] & "'")

            else:#Otherwise add it as is
                insert_vals.add(""" """" & key & """" = """ & values[key])

        else:#Otherwise this column is in the where clause(its a filter)
            #Add the column name
            match_cols.add(key)

            #Now add the value to the values list
            if self.col_types[index] == "varchar" or self.col_types[index] == "json" or self.col_types[index] == "jsonb" or self.col_types[index] == "text":
                match_vals.add("'" & values[key] & "'")
            else:
                match_vals.add(values[key])

    #Build a string to set the values of each field
    var col_string: string = "set "

    for i in 0..len(insert_cols)-1:        
        #Check if this is the last column
        if i == len(insert_cols)-1:
            col_string.add(insert_vals[i])
        else:
            col_string.add(insert_vals[i] & ",")

    #Now build the where filter
    var where_string: string = " where "

    for i in 0..len(match_cols)-1:
        #Check if this is the last column
        if i == len(match_cols)-1:
            where_string.add(""" """" & match_cols[i] & """" = """ & match_vals[i])
        else:
            where_string.add(""" """" & match_cols[i] & """" = """ & match_vals[i] & " and")

    #Now build the query string
    var querystring: string
    querystring = """update """" & self.name & """" """ & col_string & where_string & """ returning id"""

    #Run the query and get the resulting row (will contain the row id)
    let Resp = await self.pool.pool_query(querystring)

    #Now get the result as an integer
    return parseInt(Resp.result[0][0])
