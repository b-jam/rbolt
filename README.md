## rbolt

I got really inspired by LMDB, which inspired bolt, which the original maintainer "finished", so it was forked as bbolt\
As it turns out, bbolt is the storage engine at the heart of Kubernetes. (plus raft for distribution/clustering)

The reason I got so inspired is that LMDB and the rest use Copy-On-Write, one of my favorite technologies.
It's not everyday you get inspired to just write a database. I wrote a small article on copy on write here https://wattie.dev/articles/copyonwrite/

The idea with this project is doing the same thing that bolt did, just copy the code in a different language.
I love rust, so I reckon I might be able to get great performance here.

What I'm building is a b+ tree with copy on write (as opposed to an LSM tree)

Currently its just 1 database per process.

The basic idea is we are mmapping the database file, so everything is really quick.
Every 4kb page is a b+ tree node, which stores a bunch of key pointers if its a branch, or a bunch of sorted KVs if its a leaf.
Means when you get to a page, you can scan it really really quick.

I'm using zerocopy to avoid unsafe while not actually wasting time copying values around <3. Just read the page into memory once.


This is a learning project, but fun and might actually be pretty useful when done. I'll do raft later

`cargo run` for end to end testing