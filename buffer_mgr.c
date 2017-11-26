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
#include "dberror.h"
#include "storage_mgr.h"
#include "dt.h"

#include "buffer_mgr_stat.h"

/* datatype has been modified in the buffer_mgr.h
 BM_BufferPool(char *pageFile;
 int numPages;
 ReplacementStrategy strategy;
 void *mgmtData;
 BM_pageHandle *frame;
 int numRead;
 int numWrite;)
 
 BM_PageHandle(
 PageNumber pageNum;
 char *data;
 int recentHitTime;
 int dirty;
 int fixCount;)
 
 BM_List(pageNum, next)
 */

void copyPage(BM_PageHandle *const src, BM_PageHandle *const dest) {
  dest->data = src->data;
  dest->pageNum = src->pageNum;
  dest->recentHitTime = src->recentHitTime;
  dest->dirty = src->dirty;
  dest->fixCount = src->fixCount;
  dest->empty = src->empty;
}

// Buffer Manager Interface Pool Handling*********************************************************
//creates a new buffer pool with parameters which is used to cache pages from the page file with name. Initially the pool is empty and the pagefile should be existed already.
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {
  //check if pageFile exists already
  if (pageFileName == NULL) return RC_NO_FILENAME;
  SM_FileHandle fHandle;
  if (openPageFile((char *) pageFileName, &fHandle) != RC_OK) return RC_FILE_NOT_FOUND;

  // initialize the bufferpool
  bm->pageFile = (char *) pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  bm->mgmtData = stratData;
  bm->numRead = 0;
  bm->numWrite = 0;
  bm->head = NULL;
  bm->tail = NULL;

  // allocate memmory for the pageframe and initialize the frame information
  BM_PageHandle *frame = malloc(sizeof(BM_PageHandle) * numPages);
  int i = 0;
  for (i = 0; i < numPages; i++) {
    frame[i].pageNum = 0;
    frame[i].data = NULL;//data field pointer, SM_PageHandle type
    frame[i].recentHitTime = 0;
    frame[i].dirty = 0;// 1 dirty
    frame[i].fixCount = 0;
    frame[i].empty = 0;// 0 empty
  }
  bm->frame = frame;

  //8888888888888
  //fclose(fHandle.mgmtInfo);
  return RC_OK;
}

//destroy buffer pool and free up all resources associated with buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm) {

  if (bm->pageFile == NULL) return RC_NO_FILENAME;

  // make sure all the pages are written into disk
  if (forceFlushPool(bm) != RC_OK) return RC_ERROR;

  //free memory
  free(bm->frame);
  free(bm->mgmtData);

  return RC_OK;
}

// cause all dirty pages with fix count 0 from the buffer pool to be written to disk
RC forceFlushPool(BM_BufferPool *const bm) {

  if (bm->pageFile == NULL) return RC_NO_FILENAME;
  SM_FileHandle fHandle;
  if (openPageFile(bm->pageFile, &fHandle) != RC_OK) return RC_FILE_NOT_FOUND;

  //force every dirty page with fixcount 0 to write into disk
  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 1 && bm->frame[i].dirty == 1) {
      writeBlock(bm->frame[i].pageNum, &fHandle, (SM_PageHandle) (bm->frame[i].data));
      bm->frame[i].dirty = 0;
      bm->numWrite++;
    }
  }

  return RC_OK;
}

// Buffer Manager Interface Access Pages**********************************************************
//make a page dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
  //get the pageNumber in the bufferpool
  int num = page->pageNum;
  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    //find the exact page
    if (bm->frame[i].empty == 1 && bm->frame[i].pageNum == num) {

      bm->frame[i].dirty = 1;
      break;
    }
  }

  return RC_OK;
}

//unpin the page and figure out which page unpinned
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
  //get the pageNumber in the bufferpool
  int num = page->pageNum;

  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    //find the exact page
    if (bm->frame[i].empty == 1 && bm->frame[i].pageNum == num && bm->frame[i].fixCount > 0) {
      bm->frame[i].fixCount--;
      break;
    }
  }
  //printf("unpinpageHandle %d\n", page->pageNum);
  //printPoolContent(bm);
  return RC_OK;
}

//should write the page and figure out which page.
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
  if (bm->pageFile == NULL) return RC_NO_FILENAME;
  SM_FileHandle fHandle;
  if (openPageFile(bm->pageFile, &fHandle) != RC_OK) return RC_FILE_NOT_FOUND;

  //get the pageNumber in the bufferpool
  int num = page->pageNum;

  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    //find the exact page
    if (bm->frame[i].empty == 1 && bm->frame[i].pageNum == num && bm->frame[i].fixCount == 0 &&
        bm->frame[i].dirty == 1) {
      writeBlock(bm->frame[i].pageNum, &fHandle, (SM_PageHandle) (bm->frame[i].data));
      bm->frame[i].dirty = 0;
      bm->numWrite++;
      break;
    }
  }

  //8888888888888
  //fclose(fHandle.mgmtInfo);
  return RC_OK;
}

//pin the page with pageNum and figure out which page pinned
// if the page is in the bufferpool we can use it and return the address
// if not we will read it from disk and find a empty frame to store it, otherwise we will use the replacement strategy
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {
  if (bm->pageFile == NULL) return RC_NO_FILENAME;
  SM_FileHandle fHandle;
  SM_PageHandle pHandle = MAKE_SM_PAGE_HANDLE();

  RC rec = openPageFile((char *) bm->pageFile, &fHandle);
  if (rec != RC_OK) {
    printf("open page file return code: %d\n", rec);
    return RC_FILE_NOT_FOUND;
  }

  if (ensureCapacity(pageNum, &fHandle) != RC_OK) return RC_ERROR;

  // printf("pin page pageHandle %d\n", page->pageNum);
  //printPoolContent(bm);

  //get the pageNumber and content of the new page
  int num = pageNum;

  // find the page in the bufferpool and directly increase the fixcount
  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 1 && bm->frame[i].pageNum == num) {

      bm->frame[i].fixCount++;

      if (bm->strategy == RS_FIFO) {
        //do nothing about the list
      } else if (bm->strategy == RS_LRU) {// switch order of the time list
        BM_List *current = bm->head;
        BM_List *pre = NULL;

        if (current->num == bm->frame[i].pageNum) {
          bm->head = current->next;
          bm->tail->next = current;
          current->next = NULL;
          bm->tail = current;

        } else {
          while (current->num != bm->frame[i].pageNum) {
            pre = current;
            current = current->next;
          }
          pre->next = current->next;
          bm->tail->next = current;
          current->next = NULL;
          bm->tail = current;
        }
      }

      copyPage(&bm->frame[i], page);
      return RC_OK;
    }
  }

  // not find the page, and pin the page with empty frame

  if (readBlock(num, &fHandle, pHandle) != RC_OK) return RC_ERROR;
  bm->numRead++;

  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 0) {
      bm->frame[i].pageNum = num;
      bm->frame[i].data = pHandle;
      bm->frame[i].recentHitTime = 0;
      bm->frame[i].dirty = 0;
      bm->frame[i].fixCount = 1;
      bm->frame[i].empty = 1;

      // renew the time list
      BM_List *node = malloc(sizeof(BM_List));
      node->num = bm->frame[i].pageNum;
      node->next = NULL;
      if (bm->head == NULL) {
        bm->head = node;
        bm->tail = node;
      } else {
        bm->tail->next = node;
        bm->tail = node;
      }

      copyPage(&bm->frame[i], page);
      return RC_OK;
    }
  }

  // Did not find the page, and pin the page without empty, do replacement
  if (bm->strategy == RS_FIFO) {// do the FIFO replacement
    pinReplace_FIFO(bm, page, pageNum, &fHandle, pHandle);
  } else if (bm->strategy == RS_LRU) {// do the LRU replacement
    pinReplace_LRU(bm, page, pageNum, &fHandle, pHandle);
  }

  //8888888888888
  //fclose(fHandle.mgmtInfo);
  return RC_OK;
}

void pinReplace_FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,
                     const PageNumber pageNum, SM_FileHandle *fHandle, SM_PageHandle pHandle) {

  //find the pageframe that can be replaced
  PageNumber replace = bm->head->num;
  int j;
  bool fixed = false;
  for (j = 0; j < bm->numPages; j++) {
    if (bm->frame[j].empty == 1 && bm->frame[j].pageNum == replace && bm->frame[j].fixCount != 0) {
      fixed = true;
      break;
    }
  }
  if (fixed) {
    replace = bm->head->next->num;
  }

  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].pageNum == replace && bm->frame[i].dirty == 1) {
      forcePage(bm, &bm->frame[i]);
      break;
    }
  }
  //  printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  %d\n",replace);
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 1 && bm->frame[i].pageNum == replace) {

      bm->frame[i].pageNum = pageNum;
      bm->frame[i].data = pHandle;
      bm->frame[i].recentHitTime = 0;
      bm->frame[i].dirty = 0;
      bm->frame[i].fixCount = 1;
      bm->frame[i].empty = 1;


      // renew the time list
      if (fixed) {
        bm->head->next = bm->head->next->next;
        BM_List *node = malloc(sizeof(BM_List));
        node->num = bm->frame[i].pageNum;
        node->next = NULL;
        bm->tail->next = node;
        bm->tail = node;

      } else {
        bm->head = bm->head->next;
        BM_List *node = malloc(sizeof(BM_List));
        node->num = bm->frame[i].pageNum;
        node->next = NULL;
        bm->tail->next = node;
        bm->tail = node;
      }

      copyPage(&bm->frame[i], page);
      break;
    }
  }

}

void pinReplace_LRU(BM_BufferPool *const bm, BM_PageHandle *const page,
                    const PageNumber pageNum, SM_FileHandle *fHandle, SM_PageHandle pHandle) {

  //find the pageframe that can be replaced
  PageNumber replace = bm->head->num;
  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].pageNum == replace) {
      forcePage(bm, &bm->frame[i]);

    }
  }

  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 1 && bm->frame[i].pageNum == replace) {
      bm->frame[i].pageNum = pageNum;
      bm->frame[i].data = pHandle;
      bm->frame[i].recentHitTime = 0;
      bm->frame[i].dirty = 0;
      bm->frame[i].fixCount = 1;
      bm->frame[i].empty = 1;


      // renew the time list
      bm->head = bm->head->next;
      BM_List *node = malloc(sizeof(BM_List));
      node->num = bm->frame[i].pageNum;
      node->next = NULL;
      bm->tail->next = node;
      bm->tail = node;

      copyPage(&bm->frame[i], page);
      break;
    }
  }
}

// Statistics Interface****************************************************************************
PageNumber *getFrameContents(BM_BufferPool *const bm) {
  int num = bm->numPages;
  PageNumber *result = malloc(sizeof(int) * num);

  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 0) {
      result[i] = NO_PAGE;
    } else {
      result[i] = bm->frame[i].pageNum;
    }
  }
  return result;
}

//return an array of booleans
bool *getDirtyFlags(BM_BufferPool *const bm) {
  int num = bm->numPages;
  bool *result = malloc(sizeof(bool) * num);

  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 0) {
      result[i] = false;
    } else {
      // dirty 1: yes, 0: no
      if (bm->frame[i].dirty == 1) result[i] = true;
      if (bm->frame[i].dirty == 0) result[i] = false;
    }
  }
  return result;
}

//return an array of ints
int *getFixCounts(BM_BufferPool *const bm) {
  int num = bm->numPages;
  int *result = malloc(sizeof(int) * num);

  int i = 0;
  for (i = 0; i < bm->numPages; i++) {
    if (bm->frame[i].empty == 0) {
      result[i] = 0;
    } else {
      result[i] = bm->frame[i].fixCount;
    }
  }
  return result;
}

//return the number of pages read from disk since initializing the bufferpool
int getNumReadIO(BM_BufferPool *const bm) {
  return bm->numRead;
}

//return the number of pages written to the page file since initializing the bufferpool
int getNumWriteIO(BM_BufferPool *const bm) {
  return bm->numWrite;
}

