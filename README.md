# dbtrans
A database transactions wrapper in GoLang with pooling.

An useful database transactions package which exposes only three functions:

1) Open for connection as well as facility to provide connection pooling limit.
2) QueryFetch(that is both Query and Fetch in a map) for the SQL Select operations. 
3) An Exec function for the rest of the SQL operations. An additional support for connection pooling also present.
4) Supports Parameterized queries
5) All operations delimited with a Begin and End transaction capability.
