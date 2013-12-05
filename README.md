CouchDB-Bulk-Loader
===================

.NET assembly written in C# that load JSON data from a file to a CouchDB database using the **\_bulk_docs** operation

##Usage##

Pass the path to the file you want to load and press ENTER

```c-sharp
BulkLoader loader = new BulkLoader(@"C:\content.json");
```

##Changelog##

Version 0.2: Shows information about bulk process during the operation.
             Minor exception control
             
Version 0.1: Load data. Only tested with Twitter data (Close to 1 GB tweets)

##Credits##

Any question? You can find me on Twitter <a href="https://twitter.com/FitoMAD" target="_blank">@FitoMAD</a>
