# MySQL Backup

Backuper consists of pool of MySQL connections, one 
 disk writer and some controlling goroutines.
 
The pool of MySQL connections is implemented by Go library databases/sql 
controlled by connPoolSize configuration parameter. The reason of
 using limited pool of connections is tradeoff between number 
 of connections to server and speed of getting data.
 
There is only one disk writer by design. It's ok for HDD storage systems
with some number of magnetic disks.
