# DBMS
**A database management system that implements B+ and R trees**
1. the database saves records on disk by serialization thus preventing the whole database from being loaded on the RAM at once.
2. selection is implemented using binary search to improve time complexity.
3. selection can also be done efficiently using the implemented B+ and R trees.
4. the DBMS supports insertion, deletion, selection and updating of records. 
5. the join operation is not supported by this DBMS.However multiple condition selection is supported.
6. the number of records/page can be adjusted in the config file.
7. the node size of B+ tree and R tree can also be adjusted in the config file.

*the time complexity of insertion, deletion, selection and updating of a record is generally O(log(n))*
