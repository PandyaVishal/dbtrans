# dbtrans
A database transactions wrapper in GoLang with pooling.

An useful database transactions package which exposes only four functions:

1) Open for connecting to any database.
2) QueryFetch(that is both Query and Fetch in a map) for the SQL Select operations. 
3) An Exec function for the rest of the SQL operations. 
4) An Close to close the open connection and release to pool
5) Supports Connection pooling through a facility to provide connection pooling limit.
6) Supports Parameterized queries
7) All operations delimited with a Begin and an End transaction feature.

#Am currently testing it out
