#Implements the asyncronous db query objects

#Import required librarys/modules
import db_connector/db_postgres
import std/[tables, asyncdispatch, strutils, times, os, locks]
import threadpool

#Define a global lock for the postgres open function
var PGOpenLock: locks.Lock = locks.Lock()
PGOpenLock.initLock()

#Define a function that will be started in 1 or more threads to handle processing the db querys (each thread will have its own connection)
proc dbThread*(Args: PoolWorkerArgs) {.thread.}  =
    #Open a connection to the db#First create a connection to the db
    var PGConn: DbConn

    PGOpenLock.acquire()
    PGConn = db_postgres.open(Args.Creds.host, Args.Creds.user, Args.Creds.password, Args.Creds.database)
    PGOpenLock.release()

    #Now loop indefinatly waiting for querys to come in and then running them
    while true:
        #Wait for a query to come in
        var query = Args.QueryChannel[].recv()

        #Define a query result object, this will then have the returned rows added to it if there are any
        var result: QueryResult = QueryResult((id: query.id, result: @[])) 

        if query.noRet:
            #If the query is a noRet query then we just run it and return
            PGConn.exec(sql(query.query))
        else:
            #Now run the query and get the response
            var resp = PGConn.getAllRows(sql(query.query))

            #Now add the rows to the result object   
            for row in resp:
                result.result.add(row)

        #Pass the result back to the main thread
        Args.RespChannel[].send(result)

#Now define methods for the pool object
proc pool_query*(self: var PgPool, QueryString: string, noRet: bool = false): Future[QueryResult] =   
    #Create a unique id for the query
    var id = self.query_counter;

    #Create a future to return
    var f = Future[QueryResult]()

    #And increment the counter so that the next call will have a unique id
    self.query_counter += 1

    #Add the future to the result futures table
    self.ResultFutures[id] = f

    #Now define a query object that wll be passed to the db thread
    var query = Query((query: QueryString, id: id, noRet: noRet))

    #Send the query to the db thread
    self.queryChannel.send(query)

    #Return the future
    return f

# Now define a function function that will run in a loop and resolve the futures
proc ResolveFutures*(self: PgPool) {.async.} =
    #Loop indefinatly processing results as they become available
    while true:
        #Loop until there are no more results
        while true:
            # Wait for a result to come in
            var resp = self.resultChannel.tryRecv()

            if resp[0] == true:
                # Get the result
                var result = resp[1]

                # Resolve the future
                self.ResultFutures[result.id].complete(result)
            else:
                break

        await sleepAsync(1)

#Define a constructor for the pool object
proc newPgPool*(Creds: ConnCreds, ReqQueueSize: int32, RespQueueSize: int32, Size: int32): PgPool =
    #Create a new pool object
    var pool: PgPool = PgPool(creds: Creds, query_counter: 0)

    pool.queryChannel = Channel[Query]()
    pool.resultChannel = Channel[QueryResult]()
    
    #Now open both channels
    pool.queryChannel.open(ReqQueueSize)
    pool.resultChannel.open(RespQueueSize)

    #Define a table for storing the futures that need to be resolved once the query returns results
    pool.ResultFutures = initTable[int64, Future[QueryResult]]()

    #Define the args tuple that will be passed to the db threads
    var args = PoolWorkerArgs(Creds: Creds, QueryChannel: addr(pool.queryChannel), RespChannel: addr(pool.resultChannel))

    #Now create the threads
    for i in 0..Size:
        var t = Thread[PoolWorkerArgs]()
        createThread(t, dbThread, args)

    #Start the loop that will resolve the futures
    asyncCheck pool.ResolveFutures()

    #Return the pool object
    return pool
