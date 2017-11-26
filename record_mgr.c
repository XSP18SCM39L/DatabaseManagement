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
RC initRecordManager(void *mgmtData) {
  initStorageManager();
  printf("Record Manager has been initialized!\n");
  return RC_OK;
}

RC shutdownRecordManager() {
  printf("Record Manager has been shut down!\n");
  return RC_OK;
}

// creating a table should create the underlying page file and store information about the schema,free spage and so on in the table information page
RC createTable(char *name, Schema *schema) {
  //create a file for the table and it contains 1 blank page.
  if (createPageFile(name) != RC_OK) return RC_FILE_CREATION_FAILED;
  SM_FileHandle fHandle;
  SM_PageHandle fPage = (SM_PageHandle) calloc(1, PAGE_SIZE * sizeof(char));
  SM_PageHandle fPage1 = (SM_PageHandle) calloc(1, PAGE_SIZE * sizeof(char));
  if (openPageFile(name, &fHandle) != RC_OK) return RC_FILE_NOT_FOUND;

  // copy schema into the first page
  //store numAttr, keysize, size of names
  char *pointer = (char *) fPage;
  *(int *) pointer = schema->numAttr;//number of attributes
  pointer += sizeof(int);
  *(int *) pointer = schema->keySize;//number of key
  pointer += sizeof(int);
  int i;
  // Record length of the attr names
  for (i = 0; i < schema->numAttr; i++) {
    *(int *) pointer = (int) strlen(schema->attrNames[i]);
    pointer += sizeof(int);
  }
  for (i = 0; i < schema->numAttr; i++) {
    strcpy((char *) pointer, schema->attrNames[i]);
    pointer += sizeof(char) * ((int) strlen(schema->attrNames[i])); //names of attr
  }
  for (i = 0; i < schema->numAttr; i++) {
    *(int *) pointer = (int) schema->dataTypes[i]; //datatype
    pointer += sizeof(int);
  }
  for (i = 0; i < schema->numAttr; i++) {
    *(int *) pointer = schema->typeLength[i]; //typelenth especially for string
    pointer += sizeof(int);
  }
  for (i = 0; i < schema->keySize; i++) {
    *(int *) pointer = schema->keyAttrs[i]; //size of key
    pointer += sizeof(int);
  }
  *(int *) pointer = 0; //number of record/tuples
  pointer += sizeof(int);

  // the 0th page contains the information of the schema and so on
  if (writeBlock(0, &fHandle, fPage) != RC_OK) return RC_WRITE_TO_OUTPUTSTREAM_FAILED;
  // the 1st page start to store the record
  if (appendEmptyBlock(&fHandle) != RC_OK) return RC_ERROR;
  if (writeBlock(1, &fHandle, fPage1) != RC_OK) return RC_WRITE_TO_OUTPUTSTREAM_FAILED;

  if (closePageFile(&fHandle) != RC_OK) return RC_FILE_NOT_CLOSED;
  free(fPage);
  return RC_OK;
}

// all operations on a table require the table to be opened first
RC openTable(RM_TableData *rel, char *name) {
  rel->name = name;
  Schema *schema = (Schema *) malloc(sizeof(Schema));

  BM_BufferPool *bm = MAKE_POOL();
  BM_PageHandle *ph = MAKE_PAGE_HANDLE();//pined page from disk
  if (initBufferPool(bm, name, 50, RS_FIFO, NULL) != RC_OK) return RC_ERROR;

  rel->bm = bm;
  SM_FileHandle fHandle;
  if (openPageFile(name, &fHandle) != RC_OK) return RC_FILE_NOT_FOUND;
  rel->fHandle = fHandle;

  pinPage(bm, ph, 0); // find the page

  // use pointer to get different types of value
  char *pointer = ph->data;
  int numAttr = *(pointer);
  pointer += sizeof(int);
  int keySize = *(pointer);
  pointer += sizeof(int);
  int *attrNameLength = malloc(sizeof(int) * numAttr);
  int i;
  for (i = 0; i < numAttr; i++) {
    attrNameLength[i] = (int) *(pointer);
    pointer += sizeof(int);
  }
  char **attrNames = (char **) malloc(sizeof(char *) * numAttr);
  for (i = 0; i < numAttr; i++) {
    attrNames[i] = (char *) malloc(attrNameLength[i]);
    memcpy(attrNames[i], pointer, attrNameLength[i]);
    pointer += attrNameLength[i];
  }

  DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
  for (i = 0; i < numAttr; i++) {
    dataTypes[i] = (DataType) (*pointer);
    pointer += sizeof(int);
  }

  int *typeLength = malloc(sizeof(int) * numAttr);
  for (i = 0; i < numAttr; i++) {
    typeLength[i] =  *pointer;
    pointer += sizeof(int);
  }

  int *keyAttrs = (int *) malloc(sizeof(int) * keySize);
  for (i = 0; i < keySize; i++) {
    keyAttrs[i] = (int) (*pointer);
    pointer += sizeof(int);
  }
  int numtuple = (int) *(pointer);
  pointer += sizeof(int);

  schema->numAttr = numAttr;
  schema->keySize = keySize;
  schema->attrNames = attrNames;
  schema->typeLength = typeLength;
  schema->dataTypes = dataTypes;
  schema->keyAttrs = keyAttrs;

  rel->schema = schema;
  rel->bm = bm;
  rel->numTuple = numtuple;

  return RC_OK;
}

// closing a table should cause all outstanding changed to the table to be written to the page file
RC closeTable(RM_TableData *rel) {
  if (shutdownBufferPool(rel->bm) != RC_OK) return RC_ERROR;
  free(rel->bm);
  free(rel->schema->attrNames);
  free(rel->schema->dataTypes);
  free(rel->schema->keyAttrs);
  free(rel->schema->typeLength);
  free(rel->schema);

  return RC_OK;
}

RC deleteTable(char *name) {
  if (destroyPageFile(name) != RC_OK) return RC_ERROR;
  return RC_OK;
}

//return the number f tuples in the table
int getNumTuples(RM_TableData *rel) {
  return rel->numTuple;
}

/* Function for handling records in a table */
// insert a new record. assign an RID to this record and update the record parameter passed to insertRecord
RC insertRecord(RM_TableData *rel, Record *record) {
  BM_BufferPool *bm = rel->bm;
  SM_FileHandle *fHandle = &(rel->fHandle);
  BM_PageHandle *ph = MAKE_PAGE_HANDLE();

  // scan record from the page 1 to total pagenumber
  // if we can find empty slot in the existed pages just use it
  int recordSizeIndisk = sizeof(int) + getRecordSize(rel->schema);
  int numPerPage = PAGE_SIZE / recordSizeIndisk;
  int i = 1;
  while (i < rel->fHandle.totalNumPages) {
    pinPage(bm, ph, i);
    int j = 0;
    for (j = 0; j < numPerPage; j++) {
      char *pointer = ph->data + j * recordSizeIndisk;
      if (*pointer == 0) {
        *pointer = 1;
        memcpy(pointer + sizeof(int), record->data, getRecordSize(rel->schema));
        markDirty(bm, ph);

        record->id.page = i;
        record->id.slot = j;
        rel->numTuple += 1;
        return RC_OK;
      }
    }
    i++;
  }

  //if we cannot find empty slot, create a new empty page
  appendEmptyBlock(fHandle);
  pinPage(bm, ph, i);
  char *pointer = ph->data;
  *pointer = 1;
  memcpy(pointer + sizeof(int), record->data, getRecordSize(rel->schema));
  markDirty(bm, ph);

  record->id.page = i;
  record->id.slot = 0;

  rel->numTuple += 1;
  return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
  BM_BufferPool *bm = rel->bm;
  SM_FileHandle *fHandle = &(rel->fHandle);
  BM_PageHandle *ph = MAKE_PAGE_HANDLE();

  int recordInDisk = sizeof(int) + getRecordSize(rel->schema);
  int numPerPage = PAGE_SIZE/recordInDisk;

  //delete record, to mark the record unused
  if(id.page > fHandle->totalNumPages || id.slot >= numPerPage) {
    printf("DeleteRecord: RID out of range: %d, %d\n", id.page, id.slot);
    return RC_ERROR;
  }

  pinPage(bm, ph, id.page);
  char *pointer = ph->data;
  pointer += recordInDisk * id.slot;

  *(pointer) = 0;

  rel->numTuple -= 1;

  return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
  BM_BufferPool *bm = rel->bm;
  SM_FileHandle fHandle = rel->fHandle;
  BM_PageHandle *ph = MAKE_PAGE_HANDLE();

  int recordInDisk = sizeof(int) + getRecordSize(rel->schema);
  int numPerPage = PAGE_SIZE / recordInDisk;

  if (record->id.page >= fHandle.totalNumPages || record->id.slot >= numPerPage) {
    printf("UpdateRecord: RID out of range: %d, %d\n", record->id.page, record->id.slot);
    return RC_ERROR;
  }
  pinPage(bm, ph, record->id.page);
  char *pointer = ph->data;
  pointer += recordInDisk * record->id.slot;

  if (*pointer == 1) {
    memcpy(pointer + sizeof(int), record->data, getRecordSize(rel->schema));
  }

  return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
  BM_BufferPool *bm = rel->bm;
  SM_FileHandle fHandle = rel->fHandle;
  BM_PageHandle *ph = MAKE_PAGE_HANDLE();

  int recordSizeInDisk = sizeof(int) + getRecordSize(rel->schema);
  int numPerPage = PAGE_SIZE / recordSizeInDisk;

  if (id.page >= fHandle.totalNumPages || id.slot >= numPerPage) {
    printf("RID out of range: page: %d, slot: %d.\n", id.page, id.slot);
    return RC_ERROR;
  }

  pinPage(bm, ph, id.page);
  char *pointer = ph->data;
  pointer += recordSizeInDisk * id.slot;

  if (*pointer == 1) {
    record->id.page = id.page;
    record->id.slot = id.slot;
    pointer += sizeof(int);
    memcpy(record->data, pointer, getRecordSize(rel->schema));

    return RC_OK;
  }

  // Not found
  return RC_RM_RECORD_NOT_FOUND;
}

/* Functions related to scans */
// initiate a scan to retrieve all tuples from a table that fulfill a certain conditon. Starting a scan initialize the RM_ScanHandle data structure passed as an argument to startscan
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
  scan->rel = rel;
  scan->mgmtData = NULL;
  scan->page = 1;
  scan->slot = 0;
  scan->e = cond;
  return RC_OK;
}

//return the next tuple that fulfills the scan conditon. if null conditon, return all tuples
RC next(RM_ScanHandle *scan, Record *record) {
  if (record == NULL) {
    printf("scan.next: invalid record to pass back.\n");
    return RC_ERROR;
  }

  RM_TableData* rel = scan->rel;
  RID *rid = malloc(sizeof(RID));
  int recordSizeInDisk = sizeof(int) + getRecordSize(scan->rel->schema);
  int numPerPage = PAGE_SIZE / recordSizeInDisk;


  while (scan->page < rel->fHandle.totalNumPages) {
    while (scan->slot < numPerPage) {
      rid->page = scan->page;
      rid->slot = scan->slot;

      RC rc = getRecord(rel, *rid, record);
      if (rc == RC_OK) {
        Value *compareResult = malloc(sizeof(Value));

        evalExpr(record, rel->schema, scan->e, &compareResult);
        if (compareResult != NULL && compareResult->v.boolV) {
          free(compareResult);
          scan->slot += 1;
          return RC_OK;
        }
      } else if (rc == RC_RM_RECORD_NOT_FOUND) {
        // marked as deleted records, ignoring
        scan->slot += 1;
        continue;
      } else {
        printf("Scan.Next: error getting record.\n");
        return RC_ERROR;
      }

      scan->slot += 1;
    }
    scan->page += 1;
  }

  free(rid);

  return RC_RM_NO_MORE_TUPLES;
}

// closing a scan indicates to the record manager that all associated resources can be cleaned up
RC closeScan(RM_ScanHandle *scan) {
  return RC_OK;
}

/* Functions for dealing with schemas */
int getRecordSize(Schema *schema) {
  int i = 0;
  int result = 0;

  for (i = 0; i < schema->numAttr; i++) {
    switch (schema->dataTypes[i]) {
      case DT_INT:
        result = result + sizeof(int);
        break;
      case DT_FLOAT:
        result = result + sizeof(float);
        break;
      case DT_BOOL:
        result = result + sizeof(bool);
        break;
      case DT_STRING:
        result = result + schema->typeLength[i];
        break;
      default:
        break;
    }
  }
  return result;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  Schema *schema = (Schema *) malloc(sizeof(Schema));
  schema->numAttr = numAttr;
  schema->attrNames = attrNames;
  schema->dataTypes = dataTypes;
  schema->typeLength = typeLength;
  schema->keyAttrs = keys;
  schema->keySize = keySize;
  return schema;
}

RC freeSchema(Schema *schema) {
  free(schema->attrNames);
  free(schema->dataTypes);
  free(schema->keyAttrs);
  free(schema->typeLength);
  free(schema);
  return RC_OK;
}

/* Functions for dealing with records and attribute values */
// create a new record for a given schema. allocate enough memeory to data field to hold the binary representations for all attributes of this record as determined by the schema
RC createRecord(Record **record, Schema *schema) {
  *record = (Record *) malloc(sizeof(Record));
  (*record)->data = (char *) malloc(getRecordSize(schema));
  return RC_OK;
}

RC freeRecord(Record *record) {
  free(record->data);
  free(record);
  return RC_OK;
}

// Get the attribute values of a record
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
  if (schema == NULL) {
    printf("getAttr: Invalid schema.\n");
    return RC_ERROR;
  }

  if (attrNum >= schema->numAttr) {
    printf("getAttr: attrNum %d / %d is out of range of total attributes.\n", attrNum, schema->numAttr);
    return RC_ERROR;
  }

  int dataOffset = 0;
  int i;
  for (i = 0; i < attrNum; i++) {
    switch(schema->dataTypes[i]) {
      case DT_BOOL:
      case DT_FLOAT:
      case DT_INT:
        dataOffset += sizeof(int);
        break;
      case DT_STRING:
        dataOffset += schema->typeLength[i];
        break;
      default:
        printf("getAttr: invalid attribute index.\n");
        return RC_ERROR;
    }
  }

  char *target = record->data + dataOffset;

  *value = malloc(sizeof(Value));
  (*value)->dt = schema->dataTypes[attrNum];

  switch((*value)->dt) {
    case DT_BOOL:
      (*value)->v.boolV = *target;
      break;
    case DT_INT:
      (*value)->v.intV = *target;
      break;
    case DT_FLOAT:
      (*value)->v.floatV = *target;
      break;
    case DT_STRING:
      (*value)->v.stringV = malloc(schema->typeLength[attrNum]);
      memcpy((*value)->v.stringV, target, schema->typeLength[attrNum]);
      break;
    default:
      printf("getAttr: unknown data type in value: %d.\n", (*value)->dt);
      return RC_ERROR;
  }

  return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
  if (schema == NULL) {
    printf("setAttr: Invalid schema.\n");
    return RC_ERROR;
  }

  if (attrNum >= schema->numAttr) {
    printf("setAttr: attrNum %d / %d is out of range of total attributes.\n", attrNum, schema->numAttr);
    return RC_ERROR;
  }

  if (value->dt != schema->dataTypes[attrNum]) {
    printf("the data type in Schema is different from the type in value.\n");
    return RC_ERROR;
  }

  int dataOffset = 0;
  int i;
  for (i = 0; i < attrNum; i++) {
    switch(schema->dataTypes[i]) {
      case DT_BOOL:
      case DT_FLOAT:
      case DT_INT:
        dataOffset += sizeof(int);
        break;
      case DT_STRING:
        dataOffset += schema->typeLength[i];
        break;
      default:
        printf("setAttr: invalid attribute index.\n");
        return RC_ERROR;
    }
  }

  char *target = record->data + dataOffset;
  switch(value->dt) {
    case DT_BOOL:
    case DT_INT:
    case DT_FLOAT:
      memcpy(target, &(value->v.intV), sizeof(int));
      break;
    case DT_STRING:
      strcpy(target, value->v.stringV);
      break;
    default:
      printf("setAttr: unknown data type in value: %d", value->dt);
      return RC_ERROR;
  }

  return RC_OK;
}
