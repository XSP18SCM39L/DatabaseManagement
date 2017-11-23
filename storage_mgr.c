/************************************************************
 *                   CS525 Group7  2017Fall                *
 Xue Jie    Xiao Li   Tong Wu   Lei Liu
 ************************************************************/

#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>
#include <errno.h>
#include "dberror.h"
#include "storage_mgr.h"

/************************************************************
 *             interface   implementation                   *
 ************************************************************/
/* manipulating page files */

SM_FileHandle *fileHandles[MAX_NUM_OPEN_FILES]; // FileHandle of the opened fileHandles
SM_PageHandle pa;//type char * SM_PageHandle
//FILE *fp; //file pointer

typedef int bool;// define the boolean type
#define TRUE  1
#define FALSE 0

// Initialize the parameters of the storage manager with all fileHandles and file pointer NULL
extern void initStorageManager(void) {

  //initialize all parameters
  int i;
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    fileHandles[i] = NULL;
  }

  pa = NULL;
  //fp = NULL;
  //print the initializing message
  printf("Storage manager has been initialized!");

}

//Create a nonexistent file named fileName, fill the fill with an emptypage ,and if fileHandle has the same name with existed file, return error
extern RC createPageFile(char *fileName) {
  //check file name
  if (fileName == NULL) return RC_NO_FILENAME;

  //check if the file exists already
  int i;
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    if (fileHandles[i] != NULL && fileHandles[i]->fileName == fileName) {
      printf("File %s is open!", fileName);
      return RC_FILE_HANDLE_NOT_INIT;
    }
  }

  // create a new file
  FILE *fp = fopen(fileName, "w+");
  if (fp == NULL) return RC_FILE_CREATION_FAILED;
  //char onePage[PAGE_SIZE];
  //memset(onePage, '\0', PAGE_SIZE);
  //why this is not working?
  SM_PageHandle onePage = (SM_PageHandle) calloc(1, PAGE_SIZE * sizeof(char));
  int wr = (int) fwrite(onePage, sizeof(char), PAGE_SIZE, fp);

  if (wr < 0 || wr != PAGE_SIZE) {
    printf("Write error in creating file!");
    return RC_FILE_CREATION_FAILED;
  }

  // printf("Created page file, after size: %ld\n", ftell(fp));

  free(onePage);
  fclose(fp);
  return RC_OK;
}


void copyFileHandle(SM_FileHandle *const src, SM_FileHandle *const dest) {
  dest->fileName = src->fileName;
  dest->totalNumPages = src->totalNumPages;
  dest->curPagePos = src->curPagePos;
  dest->mgmtInfo = src->mgmtInfo;
}

void maintainFileHandle(SM_FileHandle *const fHandle) {
  int i;
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    if (fileHandles[i]->fileName == fHandle->fileName) {
      copyFileHandle(fHandle, fileHandles[i]);
      break;
    }
  }

}

//Open an existing file, update the fileHandle and add it to the fileHandle list. After opening the file, update the fileHandle information
extern RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
  //check the file name
  if (fileName == NULL) return RC_NO_FILENAME;

  //check the file if it's already in the open fileHandle list
  int i;
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    if (fileHandles[i] != NULL && fileHandles[i]->fileName == fileName) {
      //printf("This file is in the fileHandle list[%d], requested: %s, existing: %s\n", i, fileName, fileHandles[i]->fileName);
      copyFileHandle(fileHandles[i], fHandle);

      return RC_OK;
    }
  }

  //open a file and store the info of the file into fileHandle
  FILE *fp = fopen(fileName, "r+");
  if (fp == NULL) {
    printf("Failed to open file %s, errno: %d\n", fileName, errno);
    return RC_FILE_READ_FAILED;
  }

  // SM_FileHandle is a struct and we want to cache it so we need to have our own copy.
  SM_FileHandle *newFileHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
  newFileHandle->fileName = fileName;
  fseek(fp, 0L, SEEK_END);
  newFileHandle->totalNumPages = (int) ftell(fp) / PAGE_SIZE;
  fseek(fp, 0L, SEEK_SET);
  newFileHandle->curPagePos = 0;
  newFileHandle->mgmtInfo = fp;

  // add the new fileHandle to the filehandle list
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    if (fileHandles[i] == NULL) {
      fileHandles[i] = newFileHandle;
      copyFileHandle(newFileHandle, fHandle);
      return RC_OK;
    }
  }

  printf("No available positions in fileHandle List. Too many files opened.\n");
  return RC_FILE_HANDLE_NOT_INIT;
}

//Close a file, delete the fHandle of the file from the opened file list
extern RC closePageFile(SM_FileHandle *fHandle) {

  // Delete the corresponding file in the fileHandle list
  int i;
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    if (fileHandles[i] != NULL && fileHandles[i]->fileName == fHandle->fileName) {
      fclose(fileHandles[i]->mgmtInfo);
      free(fileHandles[i]);
      fileHandles[i] = NULL;
    }
  }

  return RC_OK;
}

//Destroy a file , delete the fileHandle from the list and unlink the name
extern RC destroyPageFile(char *fileName) {
  //check the file name
  if (fileName == NULL) return RC_NO_FILENAME;

  //delete the responding file in the fileHandle list
  int i;
  for (i = 0; i < MAX_NUM_OPEN_FILES; i++) {
    if (fileHandles[i] != NULL && fileHandles[i]->fileName == fileName) {
      fclose(fileHandles[i]->mgmtInfo);
      free(fileHandles[i]);
      fileHandles[i] = NULL;
    }
  }

  //unlink the file
  int r = remove(fileName);
  if (r < 0) {
    printf("Failed to remove file %s: %d", fileName, r);
    return RC_ERROR;
  }

  return RC_OK;
}

/* reading blocks from disc */
//Read block of the given pageNumber from disk by finding the position of the given page number
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

  if (fHandle == NULL) {
    printf("FileHandle is NULL");
    return RC_FILE_HANDLE_NOT_INIT;
  }

  // fHandle should be part of fileHandles already so do not need to test here again
//    int i;
//    bool have = 0;
//    for(i = 0; i < MAX_NUM_OPEN_FILES ; i++){
//        if(fileHandles[i]->fileName == fHandle->fileName) {
//            have = 1;
//            break;
//        }
//    }
//    if(!have){
//        //printf("ReadBlock: File not in the open list!");
//        //return RC_FILE_NOT_FOUND;
//    }

  if (pageNum < 0) {
    printf("ReadBlock: Wrong page number: %d < 0.", pageNum);
    return RC_READ_NON_EXISTING_PAGE;
  }

  if (pageNum > fHandle->totalNumPages) {
    printf("ReadBlock: Wrong page number %d > %d", pageNum, fHandle->totalNumPages);
    return RC_READ_NON_EXISTING_PAGE;
  }

  //copy onepage from disk to memPage by fread
  if (fHandle->mgmtInfo == NULL) return RC_NO_FILENAME;

  fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE * sizeof(char), SEEK_SET);
  fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

  fHandle->curPagePos = pageNum;

  maintainFileHandle(fHandle);
  return RC_OK;
}

extern int getBlockPos(SM_FileHandle *fHandle) {
  return fHandle->curPagePos;
}

//Read a page from disk in the first block
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(0, fHandle, memPage);
}

//Read a page from disk in the previous block
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (fHandle->curPagePos - 1 < 0) return RC_ERROR;
  return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

//Read a page from disk in the crrrent block
extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->curPagePos, fHandle, memPage);
}

//Read a page from disk in the next block
extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (fHandle->curPagePos + 1 > fHandle->totalNumPages) return RC_ERROR;
  return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

//Read a page from disk in the last block
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->totalNumPages, fHandle, memPage);
}

/* writing blocks to a page file */
//Write a page to disk in the given block by finding the position of the pageNumber
extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  // Same as in readBlock: fHandle should be part of fileHandles already so do not need to test here again
  //int i;

//    bool have = 0;
//    for (i = 0; i < MAX_NUM_OPEN_FILES ; i++) {
//        if(fileHandles[i] != NULL && fileHandles[i]->fileName == fHandle->fileName) {
//            have = 1;
//            break;
//        }
//    }
//    if (!have) {
//        //printf("WriteBlock: File not in the open list: %s!\n", fHandle->fileName);
//        //return RC_FILE_NOT_FOUND;
//    }

  if (fHandle == NULL) {
    printf("FileHandle is NULL");
    return RC_FILE_HANDLE_NOT_INIT;
  }

  if (memPage == NULL) {
    printf("PageHandle is NULL");
    return RC_ERROR;
  }

  if (pageNum < 0) {
    printf("WriteBlock: Wrong page number: %d < 0.\n", pageNum);
    return RC_WRITE_FAILED;
  }

  while (pageNum > fHandle->totalNumPages) {
    appendEmptyBlock(fHandle);
  }

  //copy onepage from memPage to disk by fread
  if (fHandle->mgmtInfo == NULL) return RC_NO_FILENAME;

  fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE * sizeof(char), SEEK_SET);
  fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
  fHandle->curPagePos = (int) ftell(fHandle->mgmtInfo) / PAGE_SIZE;

  maintainFileHandle(fHandle);
  return RC_OK;
}

//Write a page to disk in the crrrent block
extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

//Append an empty page to the file by writing an page behind the last page of the file
extern RC appendEmptyBlock(SM_FileHandle *fHandle) {

  // check if the file is in the opened file list
  //int i;
//    bool have = 0;
//    for(i = 0; i < MAX_NUM_OPEN_FILES ; i++){
//        if(fileHandles[i] != NULL && fileHandles[i]->fileName == fHandle->fileName) {
//            have = 1;
//            break;
//        }
//    }
//    if(!have){
//        //printf("AppendEmptyBlock: File not in the open list: %s!", fHandle->fileName);
//        //return RC_FILE_NOT_FOUND;
//    }

  //add one page to the file
  if (fHandle->mgmtInfo == NULL) return RC_NO_FILENAME;
  fseek(fHandle->mgmtInfo, 0L, SEEK_END);
  //char onePage[PAGE_SIZE];
  //memset(onePage, '\0', PAGE_SIZE);
  SM_PageHandle onePage = (SM_PageHandle) calloc(1, PAGE_SIZE * sizeof(char));
  fwrite(onePage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
  fHandle->totalNumPages++;
  fHandle->curPagePos = fHandle->totalNumPages;

  free(onePage);

  maintainFileHandle(fHandle);
  return RC_OK;

}

//If the content needed to write to file is larger than the file numberOfPages, use appendEmptyBlock to enlarge blocks enough to store the content
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

  // check if the file is in the opened file list
  // int i;
//    bool have = 0;
//    for(i = 0; i < MAX_NUM_OPEN_FILES; i++){
//        if(fileHandles[i] != NULL && fileHandles[i]->fileName == fHandle->fileName) {
//            have = 1;
//            break;
//        }
//    }
//    if(!have){
//        //printf("EnsureCapacity: File not in the open list: %s!", fHandle->fileName);
//        //return RC_FILE_NOT_FOUND;
//    }

  //add more pages to the file
  if (numberOfPages <= fHandle->totalNumPages) {
    return RC_OK;
  } else {
    int i;
    for (i = 0; i < numberOfPages - fHandle->totalNumPages; i++) {
      appendEmptyBlock(fHandle);
    }
  }

  return RC_OK;
}

