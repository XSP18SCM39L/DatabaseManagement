# Implement an database-like system from scratch and the do the database management
------------------------------------------------------------------------------------------------------------------------------------
	Design and implement a database management system by implementing and combining several functional parts.
Storage Manager: allow reading/writing of blocks to/from a file on disk
Buffer Manager: manages a buffer of blocks in memory including/flushing to disk and block replacement using replacement strategy
Record Manager: allow navigation through records, and inserting and deleting records



Please do the following operations on Linux system: 
1.  $ make clean
2.  $ make
3.  $ ./test_assign


This program contains:
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
test_assign.c
test_helper.h


