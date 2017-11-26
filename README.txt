Xiao Li  A20370639  Jie Xue A20393463  Tong Wu A20372295  Lei Liu A20288942

Please do the following operations on Linux system: ( The first step may vary, based on where you store our program in your computer)
1.  $ cd 525Group7_assign3
2.  $ make clean
3.  $ make
4.  $ ./test_assign3
Please use the command - “make clean” to clean the files created to start a new test.

The following files are all about record manager. 
Makefile
README.txt
dberror.c
dberror.h        
dt.h
storage_mgr.c
storage_mgr.h
buffer_mgr.c
buffer_mgr.h
buffer_mgr_stat.c
buffer_mgr_stat.h
expr.c
expr.h
record_mgr.c
record_mgr.h
rm_serializer.c
tables.h
test_expr.c
test_assign3_1.c
test_helper.h

We made many changes in table.h

TableData: Management Structure for a Record Manager to handle one relation

typedef struct RM_TableData
{
  char *name;
  Schema *schema;
    void *mgmtData;
  int numTuple;
  BM_BufferPool *bm;
  SM_FileHandle fHandle;
} RM_TableData;


The relations among record manager, tables and files are that: record manger manages many tables at the same time; each table is a file on disk; there’re a lot of records in each table file.

Functions used for table & manager:

RC initRecordManager(void *mgmtData)
RC shutdownRecordManager()

We create a blank page when creating a table, and we should create the underlying page files and store information about the schema,free space and so on in the table information page. Specifically, we use first page to store the schema, number of tuples and other information about the table, and we will use the the rest pages to store records. And we use the first four bytes to store a value 1 or 0 to indicate if a slot has a record. 
RC createTable(char *name, Schema *schema)

When open a table, we read the schema of the first page and store these information into the schema structure. All operations on a table require the table to be opened first. 
RC openTable(RM_TableData *rel, char *name) 

Closing a table should cause all outstanding changed to the table to be written to the page file.
RC closeTable(RM_TableData *rel)

Deleting a table will delete this file.
RC deleteTable(char *name)

Function for handling records in a table:

When inserting records to pages, we assign an RID to this record and update the record parameter passed to insertRecord. If we find an available space in the existed pages, then insert the target record into it, otherwise, we have to create a new page and insert the record into the first slot of the page. Here, we used the first four bytes to store a value “1” or “0” to indicate that 
RC insertRecord(RM_TableData *rel, Record *record) 

When deleting a record from the table file, we have to update the value from “1” to “0” to indicate that this slot has been deleted and there is no records at this slot, so that we can find a vacant slot immediately by the value “0” when a new record comes in.
RC deleteRecord(RM_TableData *rel, RID id) 

getNumTuples function will return the number of tuples of the table from the RM_TableData.
int getNumTuples(RM_TableData *rel)

Functions related to scans:

startScan function is used to retrieve all target tuples/records satisfying the searching conditions from a table. We first initialize the pointer that points to the start position. Starting a scan initialize the RM_ScanHandle data structure passed as an argument to start scan.
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)

Return the next tuple that fulfills the scan conditon. if null conditon, return all tuples
RC next(RM_ScanHandle *scan, Record *record)

Closing a scan indicates to the record manager that all associated resources can be cleaned up
RC closeScan(RM_ScanHandle *scan)

Functions for dealing with schemas:
int getRecordSize(Schema *schema)
RC freeSchema(Schema *schema)

Functions for dealing with records and attribute values:

Creating a new record for a given schema. allocate enough memeory to data field to hold the binary representations for all attributes of this record as determined by the schema.
RC createRecord(Record **record, Schema *schema)
RC freeRecord(Record *record)
getAttr function will get the values of the given attributes.
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
setAttr function will set the value of the given attributes.
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
