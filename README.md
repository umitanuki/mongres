Mongo + Postgres (with plv8) = Mongres
======================================

Mongres is a PostgreSQL extension that runs a custom background worker
speaks mongo wire protocol.  The project is inspired by
[Mogoloid](https://github.com/JerrySievert/mongolike).

Install
-------

- PostgreSQL 9.3
- PL/v8 1.3+
- mongo client

With postgres installed, make sure you have `pg_config` in your $PATH.
Then, from the shell, run

    make install

Create a database named `mongres`, and create extension plv8.

    =# create database mongres
    =# \c mongres
    =# create extension plv8;

Edit postgresql.conf accordingly.

    shared_preload_libraries = '$libdir/mongres,$libdir/plv8.so'

Finally, install functions.sql.

    psql -f functions.sql mongres

Restart postgres, and type mongo.  You should connect to mongres.

Screenshot
----------

After starting the postgres, connect to mongres, insert 3 items into test.products.

    $ mongo
    MongoDB shell version: 2.0.1
    connecting to: test
    > db.products.insert( [ { _id: 11, item: "pencil", qty: 50, type: "no.2" },
    ...                       {          item: "pen", qty: 20 },
    ...                       {          item: "eraser", qty: 25 } ] )
    Tue May 21 23:44:29 TypeError: res has no properties shell/db.js:546
    > db.products.find()
    { "_id" : 11, "item" : "pencil", "qty" : 50, "type" : "no.2" }
    { "item" : "pen", "qty" : 20, "_id" : "ea746bc5b6cd48f2638880e0" }
    { "item" : "eraser", "qty" : 25, "_id" : "ccabfa394e2acb19a56f8645" }
    > db.products.find({_id:11})
    { "_id" : 11, "item" : "pencil", "qty" : 50, "type" : "no.2" }
    >

From psql, you should be able to see the data.

    mongres=# table test.products;
                id            |                            data                             
    --------------------------+-------------------------------------------------------------
     11                       | {"_id":11,"item":"pencil","qty":50,"type":"no.2"}
     ea746bc5b6cd48f2638880e0 | {"item":"pen","qty":20,"_id":"ea746bc5b6cd48f2638880e0"}
     ccabfa394e2acb19a56f8645 | {"item":"eraser","qty":25,"_id":"ccabfa394e2acb19a56f8645"}
    (3 rows)
    


Limitation
----------

This is currently a prototype.  The following operations are supported.

- db.collection.find()
- db.collection.insert()

Any suggestions, and/or contributions are welcomed.
