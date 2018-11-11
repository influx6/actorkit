Bytes
---------
Bytes provides a delimited stream reader and writer which will encoded a sequence of giving byte set 
with a giving character set as ending, a delimiter sort of to indicate to it's reader that this is the end 
of this set. It escapes the delimiter if it appears within the byte sequence to ensure preservation. 

It is useful when multiplexing multiple streams of bytes over a connection where we wish to send 
multiple messages without the use of size headers where we prefix the size of giving stream before sequence 
of bytes, this becomes useful when you have memory restrictions and can't know total size of incoming bytes 
unless fully read out.

The delimited stream implementation has a giving caveat, which is to ensure that you do not use endless multiple 
version of delimiter within encoded stream, like sample below:

```go
Wondering out the ://////////////////////////////////////
```

When using '//' as delimiter, the writer will have no issue encoding returning encoded version:

```go
Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///
```

Unfortunately the reader will be unable to preserve total occurrence of all '/' character during decoding, leaving 
you with one less '/' character

```go
Wondering out the ://///////////////////////////////////
```

This is a rear case though, but care must usually be taking, hopefully a fix will be found for said issue soon.

