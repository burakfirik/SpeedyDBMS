SpeedyDBMS
==========
SpeedyDBMS Overview 

SimpleDB: A basic database system • What is has – Heapfiles – Basic Operators (Scan, Filter, JOIN, Aggregate)  – Buffer Pool – Transactions – SQL Front-end • Things it doesn't have – Query optimizer – Fancy relational operators (UNION, etc)  – Recovery – Indices 
Module Diagram 
TupleDesc and Tuple • TupleDesc - the schema –iterator() –Array of TDItem to iterate on, it’s ok to add fields, methods; in fact, various getter and setter will tell you what fields you need to add • Tuple –Simple, mostly one line.     
Catalog 
• Catalog stores a list of available tables, TupleDesc –void addTable(DbFile d, String n, String pk)  –DbFile getDatabaseFile(int tableid)  –TupleDesc getTupleDesc(int tableid)  • Not persisted to disk  
HeapFile.java • An array of HeapPages on disk • Javadoc is your friend! • Implement • HeapFile(File f, TupleDesc td) • readPage(PageId pid) • numPages() • iterator(TransactionId tid) 
HeapPageId.java • HeapPageId(int tableId, int pgNo) – tells you what fields you need in this class • hashCode() – we told you a way of concatenation • RecordId.java similar 
HeapPage.java 
• Format –Header is a bitmap –Page contents are an array of fixed-length Tuples • Full page size =  BufferPool.PAGE_SIZE • Number of bits in Header = number of Tuples • Header size + size of tuples = BufferPool.PAGE_SIZE 
BufferPool.java • Manages cache of pages –Evicts pages when cache is full [not lab 1] • All page accesses should use getPage –Even from inside DbFile! You will eventually implement  –locking for transactions –Flushing of pages for recovery 
SeqScan.java • Reset() – tableid, alias, iterator, tupld desc etc. • Need iterator over database file. 
Compiling, Testing, and Running • Compilation done through the ant tool – Works a lot like make • Two kinds of tests: – Unit tests – System Tests • Demo on debugging using unit tests.  
