include "../src/nim_db_pool.nim"

#Define credentials to use for connecting to the datbase, in production this should be stored in a config file
const CREDS: ConnCreds = ConnCreds(host: "localhost:26257", user: "dht_scraper", password: "ChangeMe", database: "dht_scraper")

import std/[times, monotimes]

proc main() {.async.} =
    const INSERT_TEST_COUNT = 1000

    var db = newDB(CREDS)

    #Create a table to use for testing
    await db.queryn("drop table if exists test")
    await db.queryn("create table if not exists test(id int primary key, name string)")

    #Get a reference to the table object
    var testTbl = await db.load_table("test")

    #Benchmark adding rows via insert_many
    var start = getMonoTime()
    var rows: seq[Table[string, string]] = @[]
    for i in 0..<INSERT_TEST_COUNT:
        var row: Table[string, string] = initTable[string, string]()
        row["id"] = $i
        row["name"] = "test" & $i
        rows.add(row)

    discard await testTbl.insert_many(rows)

    echo "Insert Many: " & $inMilliseconds(getMonoTime() - start)

    #Benchmark adding rows via insert
    start = getMonoTime()
    for i in INSERT_TEST_COUNT..<INSERT_TEST_COUNT*2:
        var row: Table[string, string] = initTable[string, string]()
        row["id"] = $i
        row["name"] = "test" & $i
        discard await testTbl.insert(row)
    echo "Insert: " & $inMilliseconds(getMonoTime() - start)

    #Returns the number of rows in the table, can optionally specify a where clause
    echo await testTbl.count()
    echo await testTbl.count("name ILIKE 'test%1'")

    #Remove the test table
    await db.queryn("drop table if exists test")


waitFor main()
