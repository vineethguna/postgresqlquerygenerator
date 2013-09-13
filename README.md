postgresqlquerygenerator
========================

Query Generator for postgreSQL based on psycopg2.
Reduces the overhead of writing queries

Dependencies
------------
* psycopg2 

> postgreSQL adapter for python
    
Installing psycopg2

    pip install psycopg2

Usage
-----

Import **dbOperations** and create an object of **DBOperations** class by passing **databasename, username, password** as parameters

    import dbOperations
    
    dbObj = dbOperations.DBOperations(databasename, username, password)


###CREATE TABLE

The below function will create a table and if succeeded returns True else returns False

    dbObj.createTable(tablename, schema) 


* tablename - The table Name to be created
* schema  - The table schema to be created like information about columns

###DROP TABLE

The below function will drop a table and if succeeded returns True else returns False

    dbobj.dropTable(tablename)

* tablename - The table Name to be dropped

###INSERT TUPLE OR ROW

The below function will insert a row and if succeeded returns True else returns False

    dbobj.insertTuple(tablename, fields)

* tablename - The table to which a row has to be inserted
* fields - Fields is a dictionary which contains (columnName: columnValue) mapping  
  **Ex: {"id": 10, "username": test}**    
  where "id" and "username" are column names in the table

###SELECT ROWS

The below function will select rows or row based on the parameters given

    dbobj.fetchTuples(tablename, fields, [wherecondition=None, typeOfSelectQuery=ALL_TUPLES, number=None, returnForm=None])

* tablename - The tablename from which rows are to be retrieved
* fields - The fields to be retrieved
* wherecondition - Defaults to None meaning there is no condition during retrieval you can specify condition if you want to retrieve based on condition
* typeOfSelectQuery - Default to ALL_TUPLES meaning all the rows, other valid values are RECENT_TUPLE, MANY_TUPLES
* number - This parameter is used when using MANY_TUPLES 
* returnForm - The return format of rows defaults to tuple, possible values dict

###UPDATE ROW

The below function updates a row based on WHERE condition and if succeeded returns True else returns False

    dbobj.updateTuple(tablename, updateFields, whereCondition)

* tablename - The tablename in which the row is to be updated
* updateFields - This is a dictionary similar to fields in insertTuple() 
* whereCondition - This is also a dictionary which contains a key "fields" and "condition"

###DELETE ROW

The below function deletes a row based on WHERE condition and if succeeded returns True else returns False
 
    dbobj.deleteTuple(tableName, whereCondition)

* tablename - The tablename in which the row is to be deleted
* whereCondition - This is also a dictionary which contains a key "fields" and "condition"

###CROSS JOIN

The below function gives you the cross join of the given two tables

    dbobj.crossJoin(table1, fields, table2)

* table1 - The first table name in cross join
* fields - A list of columns of table 1   Ex: ["id"]
* table2 - The second table name in cross join

###EXECUTE THE LAST QUERY

The below function executes the last successful query

    dbobj.executeLastQuery()

###OPERATORS DEFINED FOR CONDITIONS

* lt - '<'
* gt - '>'
* ne - '!='
* re - '~'
* eq - '='



##FOR EXAMPLES REGARDING THE ABOVE FUNCTIONS DO REFER testFile.py



    


