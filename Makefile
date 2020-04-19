CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

main:  y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o Statistics.o  main.o
	$(CC) -o main y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o Statistics.o  main.o -lfl -lpthread

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2test.o -lfl -lpthread

mygtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o mygtest.o
	$(CC) -o mygtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o Heap.o Sorted.o DBFile.o Pipe.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o  mygtest.o -lfl -lpthread -lgtest

mygtest.o: mygtest.cc
	$(CC) -g -c mygtest.cc
	
test.o: test.cc
	$(CC) -g -c test.cc

a2test.o: a2test.cc
	$(CC) -g -c a2test.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
