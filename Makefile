all: test_assign
test_assign: test_assign.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o rm_serializer.o test_expr.o expr.o record_mgr.o
	gcc -g test_assign.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o rm_serializer.o expr.o record_mgr.o -o test_assign

storage_mgr.o: storage_mgr.c storage_mgr.h
	gcc -g -c storage_mgr.c

test_expr.o: test_expr.c
	gcc -g -c test_expr.c

test_assign.o: test_assign.c test_helper.h
	gcc -g -c test_assign.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h
	gcc -g -c buffer_mgr.c

storage_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h
	gcc -g -c buffer_mgr_stat.c

dberror.o: dberror.c dberror.h
	gcc -g -c dberror.c

rm_serializer.o: rm_serializer.c
	gcc -g -c rm_serializer.c

expr.o: expr.c expr.h
	gcc -g -c expr.c

record_mgr.o: record_mgr.c record_mgr.h
	gcc -g -c record_mgr.c

clean:
	rm buffer_mgr.o test_expr.o dberror.o test_assign.o storage_mgr.o buffer_mgr_stat.o rm_serializer.o expr.o record_mgr.o test_assign

run-test: test_assign
	./test_assign