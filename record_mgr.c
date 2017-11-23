/************************************************************
 *                   CS525 Group7  2017Fall                *
 Jie Xue    Xiao Li   Tong Wu   Lei Liu
 ************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "dt.h"
#include "expr.h"
#include "tables.h"
#include "record_mgr.h"


/* Functions for table and manager */
RC initRecordManager (void *mgmtData){
    initStorageManager();
    printf("RecordManager has been initialized!");
    return RC_OK;
}

RC shutdownRecordManager (){
    printf("RecordManager has been shut down!");
    return RC_OK;
}

// creating a table should create the underlying page file and store information about the schema,free spage and so on in the table information page
RC createTable (char *name, Schema *schema){
    //create a file for the table and it contains 1 blank page.
    if(createPageFile(name)!= RC_OK) return RC_FILE_CREATION_FAILED;
    SM_FileHandle fHandle;
    SM_PageHandle fPage = (SM_PageHandle) calloc(1, PAGE_SIZE * sizeof(char));
    SM_PageHandle fPage1 = (SM_PageHandle) calloc(1, PAGE_SIZE * sizeof(char));
    if(openPageFile(name, &fHandle) != RC_OK) return RC_FILE_NOT_FOUND;
    
    //copy schema into the first page
    /*
    sprintf((char*)fPage, "%d", schema->numAttr);
    int i = 0;
    while(i < schema->numAttr){
        sprintf((char*)fPage, "%s%s", (char*)fPage, schema->attrNames[i]);
        i++;
    }
    i = 0;
    while(i < schema->numAttr){
        sprintf((char*)fPage, "%s%d", (char*)fPage, schema->dataTypes[i]);
        i++;
    }
    i = 0;
    while(i < schema->numAttr){
        sprintf((char*)fPage, "%s%d", (char*)fPage, schema->typeLength[i]);
        i++;
    }
    i = 0;
    while(i < schema->keySize){
        sprintf((char*)fPage, "%s%d", (char*)fPage, schema->keyAttrs[i]);
        i++;
    }
    sprintf((char*)fPage, "%s%d", (char*)fPage, schema->keySize);
    sprintf((char*)fPage, "%s%d", (char*)fPage, 0);*/
    
    //store numAttr, keysize, size of names
    char *pointer = (char*)fPage;
    *(int *)pointer = schema->numAttr;//number of attributes
    pointer += sizeof(int);
    *(int *)pointer = schema->keySize;//number of key
    pointer += sizeof(int);
    int i;
    for(i=0;i<schema->numAttr;i++){
        *(int *)pointer = (int) strlen(schema->attrNames[i]);//number of the attrnames
        pointer += sizeof(int);
    }
    for(i=0;i<schema->numAttr;i++){
        strcpy((char*)pointer, schema->attrNames[i]);
        pointer += sizeof(char) *((int) strlen(schema->attrNames[i])+1);//names of attr
    }
    for(i=0;i<schema->numAttr;i++){
        *(int *)pointer = (int)schema->dataTypes[i];//datatype
        pointer += sizeof(int);
    }
    for(i=0;i<schema->numAttr;i++){
        *(int *)pointer = (int)schema->typeLength[i];//typelenth especially for string
        pointer += sizeof(int);
    }
    for(i=0;i<schema->keySize;i++){
        *(int *)pointer = (int)schema->keyAttrs[i];//size of key
        pointer += sizeof(int);
    }
    *(int *)pointer = 0;//number of record/tuples
    pointer += sizeof(int);
    
    // the 0 page contains the information of the schema and so on
    if(writeBlock(0, &fHandle, fPage) != RC_OK) return RC_WRITE_TO_OUTPUTSTREAM_FAILED;
    // the 1 page start to store the record
    if(appendEmptyBlock(&fHandle)!= RC_OK) return RC_ERROR;
    if(writeBlock(1, &fHandle, fPage1) != RC_OK) return RC_WRITE_TO_OUTPUTSTREAM_FAILED;
    
    if(closePageFile(&fHandle)!=RC_OK) return RC_FILE_NOT_CLOSED;
    free(fPage);
    return RC_OK;
}

// all operations on a table require the table to be opened first
RC openTable (RM_TableData *rel, char *name){
    rel->name = name;
    Schema * schema = (Schema *)malloc(sizeof(Schema));
    
     BM_BufferPool *bm = MAKE_POOL();
     BM_PageHandle *ph = MAKE_PAGE_HANDLE();//pined page from disk
     if(initBufferPool(bm, name, 50, RS_FIFO, NULL)!= RC_OK) return RC_ERROR;
     pinPage(bm, ph, 0);// find the page
    
    /*read schema and numtuple
     Schema *result;
     char *names[] = { "a", "b", "c" };
     DataType dt[] = { DT_INT, DT_STRING, DT_INT };
     int sizes[] = { 0, 4, 0 };
     int keys[] = {0};
     int i;
     char **cpNames = (char **) malloc(sizeof(char*) * 3);
     DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
     int *cpSizes = (int *) malloc(sizeof(int) * 3);
     int *cpKeys = (int *) malloc(sizeof(int));
     for(i = 0; i < 3; i++)
     {
     cpNames[i] = (char *) malloc(2);
     strcpy(cpNames[i], names[i]);
     }
     memcpy(cpDt, dt, sizeof(DataType) * 3);
     memcpy(cpSizes, sizes, sizeof(int) * 3);
     memcpy(cpKeys, keys, sizeof(int));
     
     result = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);*/
    //use pointer to get different type of value
    char *pointer = ph->data;
    int numAttr = *(pointer);
    pointer += sizeof(int);
    int keySize = *(pointer);
    pointer += sizeof(int);
    int *nameLength = malloc(sizeof(int)*numAttr);
    int i;
    for(i=0;i<numAttr;i++){
        nameLength[i] = (int)*(pointer);
        pointer += sizeof(int);
    }
    char **attrNames = (char **) malloc(sizeof(char*) * numAttr);
    for(i=0;i<numAttr;i++){
        attrNames[i] = (char *) malloc(nameLength[i]);
        strcpy(attrNames[i], (char*)pointer);
        pointer += nameLength[i];
    }
    DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
    for(i=0;i<numAttr;i++){
        dataTypes[i] = (DataType)(*pointer);
        pointer += sizeof(int);
    }
    int *keyAttrs = (int *) malloc(sizeof(int) * keySize);
    for(i=0;i<keySize;i++){
        keyAttrs[i] = (int)(*pointer);
        pointer += sizeof(int);
    }
    int numtuple = (int)*(pointer);
    pointer += sizeof(int);
    
    schema->numAttr = numAttr;
    schema->keySize = keySize;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->keyAttrs = keyAttrs;
    
    rel->schema = schema;
    rel->mgmtData = bm;
    rel->numTuple = numtuple;
    
    return RC_OK;
}

// closting a table should cause all outstanding changed to the table to be written to the page file
RC closeTable (RM_TableData *rel){
    if(shutdownBufferPool(rel->mgmtData)!=RC_OK) return RC_ERROR;
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->keyAttrs);
    free(rel->schema->typeLength);
    free(rel->schema);
    free(rel);
    return RC_OK;
}

RC deleteTable (char *name){
    if(destroyPageFile(name)!=RC_OK) return RC_ERROR;
    return RC_OK;
}

//return the number f tuples in the table
int getNumTuples (RM_TableData *rel){
    return rel->numTuple;
}

/* Function for handling records in a table */
// insert a new record. assign an RID to this record and update the record parameter passed to insertRecord
RC insertRecord (RM_TableData *rel, Record *record){
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id){
    
    return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record){
    
    return RC_OK;
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
    
    return RC_OK;
}

/* Functions related to scans */
// initiate a scan to retrieve all tuples from a table that fullfill a certain conditon. Starting a scan initialize the RM_ScanHandle data structure passed as an argument to startscan 
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    scan->rel = rel;
    scan->mgmtData = NULL;
    scan->page = 1;
    scan->slot = 1;
    scan->e = cond;
    return RC_OK;
}

//return the next tuple that fulfills the scan conditon. if null conditon, return all tuples
RC next (RM_ScanHandle *scan, Record *record){
    
    return RC_OK;
}

// closing a scan indicates to the record manager that all associated resources can be cleaned up
RC closeScan (RM_ScanHandle *scan){
    
    return RC_OK;
}

/* Functions for dealing with schemas */
int getRecordSize (Schema *schema){
    int i = 0;
    int result = 0;

    for(i = 0;i<schema->numAttr;i++){
        switch(schema->dataTypes[i]){
            case DT_INT: result = result + sizeof(int);
                break;
            case DT_FLOAT: result = result + sizeof(float);
                break;
            case DT_BOOL: result = result + sizeof(bool);
                break;
            case DT_STRING: result = result + schema->typeLength[i];
                break;
            default: break;
        }
    }
    return result;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema * schema = (Schema *)malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength =typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}

RC freeSchema (Schema *schema){
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->keyAttrs);
    free(schema->typeLength);
    free(schema);
    return RC_OK;
}

/* Functions for dealing with records and attribute values */
// create a new record for a given schema. allocate enough memeory to data field to hold the binary representations for all attributes of this record as determined by the schema
RC createRecord (Record **record, Schema *schema){
    *record = (Record *) malloc(sizeof(Record));
    (*record)->data = (char*) malloc(getRecordSize(schema));
    return RC_OK;
}
RC freeRecord (Record *record){
    free(record);
    return RC_OK;
}

// get the attribute values of a record
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    
    return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    
    return RC_OK;
}
