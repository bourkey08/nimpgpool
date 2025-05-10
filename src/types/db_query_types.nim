#Type definitions for the query objects
#Define a type for a connection credentials object
type ConnCreds* = object
    host*: string
    user*: string
    password*: string
    database*: string

#Define a type for passing the query to the db threads
type Query* = tuple[query: string, id: int64, noRet: bool]

#Define a type for the returned query result
type QueryResult* = tuple[id: int64, result: seq[seq[string]]]

#Define a type for the arguments passed to the db thread
type PoolWorkerArgs = ref object
    Creds*: ConnCreds
    QueryChannel*: ptr Channel[Query]
    RespChannel*: ptr Channel[QueryResult]

#Define a type for storing the query pool object
type PgPool* = ref object
    queryChannel: Channel[Query]
    resultChannel: Channel[QueryResult]
    query_counter: int64
    creds: ConnCreds
    ResultFutures: Table[int64, Future[QueryResult]]
