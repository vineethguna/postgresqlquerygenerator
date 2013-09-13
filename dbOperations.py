__author__ = 'vineeth'

import psycopg2
import psycopg2.extras

#Insert Tuple into database
#    Required Fields
#    -----------------
#    CREATE TABLE                -       TableName, Fields, Type, Size
#    DROP TABLE                  -       TableName
#    ADD ROW                     -       TableName, Fields, FieldValues
#    UPDATE ROW                  -       TableName, updateFields, updateFieldValues, whereCondition
#    DELETE ROW                  -       TableName, whereCondition

conn = None
ALL_TUPLES = "ALL"
RECENT_TUPLE = "RECENT"
MANY_TUPLES = "MANY"
DICT_FORM = "dict"
LIST_FORM = "list"


class DBOperations(object):
    DATABASE_NAME = None
    DATABASE_USER = None
    DATABASE_PASSWORD = None
    DATABASE_HOST = ""
    DATABASE_PORT = ""
    INTERNAL_ERROR_STATEMENT = "Internal Error Occurred"
    CREATE_BASE_QUERY = "CREATE TABLE %s (%s);"
    DROP_BASE_QUERY = "DROP TABLE IF EXISTS %s;"
    INSERT_BASE_QUERY = "INSERT INTO %s(%s) VALUES(%s);"
    UPDATE_BASE_QUERY = "UPDATE %s SET %s WHERE %s;"
    DELETE_BASE_QUERY = "DELETE FROM %s WHERE %s;"
    SELECT_BASE_QUERY = "SELECT %s FROM %s %s %s;"
    CROSS_JOIN_BASE_QUERY = "SELECT %s FROM %s CROSS JOIN %s;"
    UPDATE_OPERATORS = {"add": "%(field)s + %(value)s",
                        "sub": "%(field)s - %(value)s",
                        "mul": "%(field)s * %(value)s",
                        "div": "%(field)s / %(value)s",
                        }
    OPERATORS = {'lt': '<', 'gt': '>', 'ne': '!=', 're': '~', 'eq': '='}
    lastExecutedQuery = None

    def __init__(self, database_name, database_user, database_password):
        try:
            self.DATABASE_NAME = database_name
            self.DATABASE_USER = database_user
            self.DATABASE_PASSWORD = database_password
            global conn
            if conn is None:
                self._connectToDatabase()
            print "Successfully Connected to Database"
        except psycopg2.DatabaseError:
            print "Error Occurred while connecting to Database"

    def _connectToDatabase(self):
        global conn
        conn = psycopg2.connect(
            database=self.DATABASE_NAME,
            user=self.DATABASE_USER,
            password=self.DATABASE_PASSWORD,
            #host=self.DATABASE_HOST,
            #port=self.DATABASE_PORT
        )
        print conn

    def commit(self):
        conn.commit()

    def rollback(self):
        conn.rollback()

    def executeQuery(self, query):
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()

    def _mogrify(self, baseQueryString, args):
        cursor = conn.cursor()
        queryString = cursor.mogrify(baseQueryString, args)
        return queryString

    def executeSelectQuery(self, query, typeOfSelectQuery, number=None, typeOfCursor=None):
        if typeOfCursor == DICT_FORM:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute(query)
        if typeOfSelectQuery == ALL_TUPLES:
            tuples = cursor.fetchall()
        elif typeOfSelectQuery == RECENT_TUPLE:
            tuples = cursor.fetchone()
        elif typeOfSelectQuery == MANY_TUPLES:
            if number is not None:
                tuples = cursor.fetchmany(number)
            else:
                tuples = cursor.fetchmany()
        else:
            tuples = ()
        cursor.close()
        return tuples

    def _returnOperator(self, opTag):
        if opTag in self.OPERATORS.keys():
            return self.OPERATORS[opTag]
        else:
            return "="

    def _returnSETOperator(self, setData):
        if "setTag" in setData:
            return self.UPDATE_OPERATORS[setData["setTag"]] % setData
        else:
            return self.UPDATE_OPERATORS["add"] % setData

    def _handleWhere(self, condition, whereConditionFields):
        whereFieldValues = []
        temp = []
        for whereField in whereConditionFields:
            conditions = whereField.split("__")
            if len(conditions) == 2:
                temp.append(conditions[0] + self._returnOperator(conditions[1]) + "%s")
            else:
                temp.append(conditions[0] + "=%s")
            whereFieldValues.append(whereConditionFields[whereField])
        if condition is not None:
            condition = " " + condition + " "
            whereString = condition.join(temp)
        else:
            whereString = "".join(temp)
        whereString = self._mogrify(whereString, whereFieldValues)
        return whereString

    def _handleSET(self, updateFields):
        temp = []
        updateFieldValues = []
        for fieldName in updateFields:
            conditions = fieldName.split("__")
            if len(conditions) == 2:
                setData = {"setTag": conditions[1], "field": conditions[0], "value": updateFields[fieldName]}
                temp.append(conditions[0] + "=" + self._returnSETOperator(setData))
            else:
                temp.append(conditions[0] + "=%s")
                updateFieldValues.append(updateFields[fieldName])
        setString = ",".join(temp)
        setString = self._mogrify(setString, updateFieldValues)
        return setString

    def closeDatabaseConnection(self):
        if conn is not None:
            conn.close()
            print "Connection Successfully Closed"
        else:
            print "Connection Not Established"

    def createTable(self, tableName, schema):
        if tableName and schema is not None:
            tableInfo = {"tableName": tableName, "schema": schema}
            queryString = self._createQuery("CREATE", tableInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    self.executeQuery(queryString)
                    self.commit()
                    print "Table Created"
                    self.lastExecutedQuery = queryString
                    return True
                except psycopg2.DatabaseError as e:
                    print "Error:" + e
                    self.rollback()
                    print "Table Creation Failed"
                    return False
            else:
                print self.INTERNAL_ERROR_STATEMENT
                return False
        else:
            print self.INTERNAL_ERROR_STATEMENT
            return False

    def dropTable(self, tableName):
        if tableName is not None:
            tableInfo = {"tableName": tableName}
            queryString = self._createQuery("DROP", tableInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    self.executeQuery(queryString)
                    self.commit()
                    print "Table Dropped"
                    self.lastExecutedQuery = queryString
                    return True
                except psycopg2.DatabaseError as e:
                    print "Error: " + str(e)
                    self.rollback()
                    print "Table Deletion Failed"
                    return False
            else:
                print self.INTERNAL_ERROR_STATEMENT
                return False
        else:
            print self.INTERNAL_ERROR_STATEMENT
            return False

    def insertTuple(self, tableName, fields):
        if tableName and fields is not None:
            tupleInfo = {"tableName": tableName, "fields": fields}
            queryString = self._createQuery("INSERT", tupleInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    self.executeQuery(queryString)
                    self.commit()
                    print "Tuple Added"
                    self.lastExecutedQuery = queryString
                    return True
                except psycopg2.DatabaseError as e:
                    print "Error: " + str(e)
                    self.rollback()
                    print "Tuple addition failed"
                    return False
            else:
                print self.INTERNAL_ERROR_STATEMENT
                return False
        else:
            print self.INTERNAL_ERROR_STATEMENT
            return False

    def fetchTuples(self, tableName, fields, where=None, typeOfSelectQuery=ALL_TUPLES, number=None, returnForm=None):
        if tableName and fields is not None:
            tableInfo = {"tableName": tableName, "fields": fields, "where": where}
            queryString = self._createQuery("SELECT", tableInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    tuples = self.executeSelectQuery(queryString, typeOfSelectQuery, number, returnForm)
                    return tuples
                except psycopg2.DatabaseError as e:
                    print "Error: " + str(e)
                    print "Retrieval Failed"
            else:
                print self.INTERNAL_ERROR_STATEMENT
        else:
            print self.INTERNAL_ERROR_STATEMENT

    def deleteTuple(self, tableName, whereCondition):
        if tableName and whereCondition is not None:
            tupleInfo = {"tableName": tableName, "where": whereCondition}
            queryString = self._createQuery("DELETE", tupleInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    self.executeQuery(queryString)
                    self.commit()
                    print "Tuple Deleted"
                    self.lastExecutedQuery = queryString
                    return True
                except psycopg2.DatabaseError as e:
                    print "Error: " + str(e)
                    self.rollback()
                    print "Tuple Deletion failed"
                    return False
            else:
                print self.INTERNAL_ERROR_STATEMENT
                return False
        else:
            print self.INTERNAL_ERROR_STATEMENT
            return False

    def updateTuple(self, tableName, updateFields, whereCondition):
        if tableName and updateFields and whereCondition is not None:
            tupleInfo = {"tableName": tableName, "updateFields": updateFields, "where": whereCondition}
            queryString = self._createQuery("UPDATE", tupleInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    self.executeQuery(queryString)
                    self.commit()
                    print "Tuple Updated"
                    self.lastExecutedQuery = queryString
                    return True
                except psycopg2.DatabaseError as e:
                    print "Error: " + str(e)
                    self.rollback()
                    print "Tuple Updation failed"
                    return False
            else:
                print self.INTERNAL_ERROR_STATEMENT
                return False
        else:
            print self.INTERNAL_ERROR_STATEMENT
            return False

    def crossJoin(self, table1, fields, table2, resultForm=None):
        if table1 and fields and table2 is not None:
            tableInfo = {"table1": table1, "fields": fields, "table2": table2}
            queryString = self._createQuery("CROSS_JOIN", tableInfo)
            if queryString != self.INTERNAL_ERROR_STATEMENT:
                try:
                    resultData = self.executeSelectQuery(queryString, ALL_TUPLES, typeOfCursor=resultForm)
                    return resultData
                except psycopg2.DatabaseError as e:
                    print "Error: " + str(e)
                    self.rollback()
                    print "Join Operation Failed"
            else:
                print self.INTERNAL_ERROR_STATEMENT
        else:
            print self.INTERNAL_ERROR_STATEMENT

    def _createQuery(self, typeOfQuery, extraInfo):
        if typeOfQuery == "INSERT":
            return self._generateInsertTupleQuery(extraInfo)
        elif typeOfQuery == "DELETE":
            return self._generateDeleteTupleQuery(extraInfo)
        elif typeOfQuery == "UPDATE":
            return self._generateUpdateTupleQuery(extraInfo)
        elif typeOfQuery == "CREATE":
            return self._generateCreateTableQuery(extraInfo)
        elif typeOfQuery == "DROP":
            return self._generateDropTableQuery(extraInfo)
        elif typeOfQuery == "SELECT":
            return self._generateSelectTableQuery(extraInfo)
        elif typeOfQuery == "CROSS_JOIN":
            return self._generateCrossJoinTableQuery(extraInfo)
        else:
            return self.INTERNAL_ERROR_STATEMENT

    def _generateCreateTableQuery(self, tableInfo):
        if isinstance(tableInfo, dict):
            tableName = tableInfo["tableName"]
            schema = tableInfo["schema"]
            queryString = self.CREATE_BASE_QUERY % (tableName, schema)
            return queryString
        else:
            return self.INTERNAL_ERROR_STATEMENT

    def _generateDropTableQuery(self, tableInfo):
        if tableInfo is not None:
            tableName = tableInfo["tableName"]
            queryString = self.DROP_BASE_QUERY % (tableName, )
            return queryString
        else:
            return self.INTERNAL_ERROR_STATEMENT

    def _generateSelectTableQuery(self, tableInfo):
        if tableInfo is not None:
            tableName = tableInfo["tableName"]
            if isinstance(tableInfo["fields"], list):
                fieldNames = ",".join(tableInfo["fields"])
            elif tableInfo["fields"] == "*":
                fieldNames = "*"
            else:
                return "Given Parameters Invalid"
            if tableInfo["where"] is not None and "condition" and "fields" in tableInfo["where"].keys():
                whereString = self._handleWhere(tableInfo["where"]["condition"], tableInfo["where"]["fields"])
                queryString = self.SELECT_BASE_QUERY % (fieldNames, tableName, "WHERE", whereString)
            else:
                queryString = self.SELECT_BASE_QUERY % (fieldNames, tableName, "", "")
            return queryString
        else:
            return self.INTERNAL_ERROR_STATEMENT

    def _generateInsertTupleQuery(self, tupleInfo):
        if tupleInfo is not None:
            tableName = tupleInfo["tableName"]
            fieldCount = len(tupleInfo["fields"])
            fieldNames = ",".join(tupleInfo["fields"].keys())
            fieldValues = tupleInfo["fields"].values()
            temp = ""
            while fieldCount != 1:
                temp += "%s,"
                fieldCount -= 1
            temp += "%s"
            queryString = self.INSERT_BASE_QUERY % (tableName, fieldNames, temp)
            queryString = self._mogrify(queryString, fieldValues)
            return queryString
        else:
            return self.INTERNAL_ERROR_STATEMENT

    def _generateUpdateTupleQuery(self, tupleInfo):
        if isinstance(tupleInfo, dict):
            tableName = tupleInfo["tableName"]
            updateFieldDict = tupleInfo["updateFields"]
            whereConditionFields = tupleInfo["where"]["fields"]
            conditionValue = tupleInfo["where"]["condition"]
            setString = self._handleSET(updateFieldDict)
            whereString = self._handleWhere(conditionValue, whereConditionFields)
            queryString = self.UPDATE_BASE_QUERY % (tableName, setString, whereString)
            return queryString
        else:
            return self.INTERNAL_ERROR_STATEMENT

    def _generateDeleteTupleQuery(self, tupleInfo):
        if isinstance(tupleInfo, dict):
            tableName = tupleInfo["tableName"]
            whereConditionFields = tupleInfo["where"]
            if "fields" and "condition" in whereConditionFields.keys():
                whereConditionFields = tupleInfo["where"]["fields"]
                conditionValue = tupleInfo["where"]["condition"]
                whereString = self._handleWhere(conditionValue, whereConditionFields)
                queryString = self.DELETE_BASE_QUERY % (tableName, whereString)
            else:
                queryString = self.DELETE_BASE_QUERY % (tableName, "")
            return queryString

    def _generateCrossJoinTableQuery(self, tableInfo):
        if tableInfo is not None:
            table1 = tableInfo["table1"]
            fields = tableInfo["fields"]
            table2 = tableInfo["table2"]
            queryString = self.CROSS_JOIN_BASE_QUERY % (",".join(fields), table1, table2)
            return queryString
        else:
            return self.INTERNAL_ERROR_STATEMENT

    #executes the last query executed on db
    def executeLastQuery(self):
        try:
            if self.lastExecutedQuery is not None:
                self.executeQuery(self.lastExecutedQuery)
            else:
                print "There are no executed queries till now"
        except psycopg2.ProgrammingError:
            self.rollback()
            print "Error executing the Query"