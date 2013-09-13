__author__ = 'vineeth'

import dbOperations


#creating dbOperations Object
dbobj = dbOperations.DBOperations("managerdb", "vineeth", "vineeth")

#creating new table
queryStatus = dbobj.createTable("sampledb", "id INTEGER PRIMARY KEY, name varchar(30)")
print queryStatus

#Inserting tuple into table
queryStatus = dbobj.insertTuple("sampledb", {"id": 10, "name": "vineeth"})
print queryStatus

#Selecting all columns from table
returnData = dbobj.fetchTuples("sampledb", "*")
print returnData

#Selecting specific columns from table
returnData = dbobj.fetchTuples("sampledb", ["id"])
print returnData

#Selecting columns based on a condition
returnData = dbobj.fetchTuples("sampledb", ["id"], {"id__lt": 15, "condition": None})
print returnData

#Selecting columns and returning it as a dict
returnData = dbobj.fetchTuples("sampledb", "*", returnForm=dbOperations.DICT_FORM)
print returnData

#Selecting recent added column
returnData = dbobj.fetchTuples("sampledb", "*", typeOfSelectQuery=dbOperations.RECENT_TUPLE)
print returnData

#Updating tuple in table
queryStatus = dbobj.updateTuple("sampledb", {"id": 11, "name": "testing"}, {"fields": {"id": 11}, "condition": None})
print queryStatus

#Updating tuple in table using condition
queryStatus = dbobj.updateTuple("sampledb", {"id": 11, "name": "testing"}, {"fields": {"id__lt": 15, "name": "test"},
                                                                            "condition": "OR"})
print queryStatus

#Deleting tuple in table
queryStatus = dbobj.deleteTuple("sampledb", {"fields": {"id": 11}, "condition": None})
print queryStatus

#Cross join query
queryStatus = dbobj.crossJoin("sampledb", ["id"], "userdetails")
print queryStatus

#Drop table in database
queryStatus = dbobj.dropTable("sampledb")
print queryStatus

#Executing Last Executed Query
dbobj.executeLastQuery()
