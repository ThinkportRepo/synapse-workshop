-- Excersize Instructions and Code
-- ######################################################
/* 

# Task 1: Explore the files for flightradar and airport data
######################################################
- open the data explorer and browse the curated folder on the dlstraind002 storage account
- checkout the file and folder structure
    - which format are the files stored
    - aren they partitioned if yes how are they partitioned?
    - what options do I have to explore themn

*/





/* 

# Task 2: explore the parquet files in capa/airport_files
######################################################
- right click on ONE Parquet file of the airport_files and select 'New SQL Script' -> 'Select TOP 100 rows'
- understand the SQL Code and execute
*/

-- SOLUTION:
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://dlstraind002.dfs.core.windows.net/curated/standardized/api/capa/airport.delta/part-00000-81938f89-7968-4e26-8c28-3789d8533fe4-c000.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

/*

# Task 3: explore the delta files in capa/airport_files
######################################################
- right click on the delta table folder of the airport_files and select 'New SQL Script' -> 'Select TOP 100 rows'
- specify the correct delta format
- run the code and compare the result

*/

-- SOLUTION:   
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://dlstraind002.dfs.core.windows.net/curated/standardized/api/capa/airport.delta/',
        FORMAT = 'DELTA'
    ) AS [result]


-- BONUS: Check if the rows are the same
SELECT
    'Parquet' as Format, COUNT(*) as Number
FROM
    OPENROWSET(
        BULK 'https://dlstraind002.dfs.core.windows.net/curated/standardized/api/capa/airport.delta/part-00000-81938f89-7968-4e26-8c28-3789d8533fe4-c000.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
UNION 
SELECT
    'Delta' as Format, COUNT(*) as Number
FROM
    OPENROWSET(
        BULK 'https://dlstraind002.dfs.core.windows.net/curated/standardized/api/capa/airport.delta/',
        FORMAT = 'DELTA'
    ) AS [result]

/*

# Task 4: do the same with the flightradar files
######################################################
- Is it working the same? What is the difference on partitioned files
*/

-- SOLUTION: 


/* 
# Task 5: Read one specific Parquet file from flightradar data
######################################################
- understand, complete and run the following code
- what is the problem? What data got lost?
*/

-- SOLUTION: one single file does only contain some rows of the dataset and looses the partition column date
SELECT
    TOP 100 a.filename() as date,*
FROM
    OPENROWSET(
        BULK 'https://dlstiscd001.dfs.core.windows.net/curated/standardized/api/flightradar24/flights.delta/date=2014-07-01/<NAME OF ONE FILE>.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS a



/* 
# Task 6: Try to extract the date from the filepath
######################################################
- When reading directly from Parquet files the partition column is lost
- https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-files

- The date value has to be reconstructed from the filename string
- the synapse sql provides a special filename() and filepath() function to work directly on files
*/

SELECT
    TOP 100 
    a.filepath() as path,
    SUBSTRING(
        a.filepath(), 
        CHARINDEX('date=', a.filepath()) + 5,  -- Start after 'date=' (5 characters)
        10  -- Extract exactly 'YYYY-MM-DD' (10 characters)
    ) AS date,
    *
FROM
    OPENROWSET(
        BULK 'https://dlstiscd001.dfs.core.windows.net/curated/standardized/api/flightradar24/flights.delta/date=2014-07-01/part-00000-3db961c5-c332-45bc-94d7-42a458472d8c.c000.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS a



/* 
# Task 7: Read all parquet files with wildcard patterns and extract and filter on date partition
######################################################
-- query multiple files with wild cards
-- no date column as the folder partition is not correctly interpreted as new columm
-- date column has to be generated manually with filepath parsing
- the synapse sql provides a special filename() and filepath() function to work directly on files

*/
SELECT 
    TOP 100 a.filepath(1) as date,*
FROM
    OPENROWSET(
        BULK 'https://dlstiscd001.dfs.core.windows.net/curated/standardized/api/flightradar24/flights.delta/date=*/*.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS a
WHERE a.filepath(1) = '2015-10-02'



/* 
# Task 8: Use the Delta reader instead
######################################################
-- using delta reader 
-- query and filter directly on date partition

*/

SELECT
    date,*
FROM
    OPENROWSET(
        BULK 'https://dlstiscd001.dfs.core.windows.net/curated/standardized/api/flightradar24/flights.delta/',
        FORMAT = 'delta'
    ) AS [result]
    where date = '2015-10-02'




/* 
# TAKE AWAY
######################################################
reading from files directly needs knowledge how the files are structured and which data I need
SQL Code is complex due to special functions and reader configurations that has to be set corectly
*/
SELECT * FROM [airport];

