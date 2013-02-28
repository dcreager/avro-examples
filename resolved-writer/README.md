## Resolved writers

This example shows how to write data into an Avro container file, and how to
read data from a file, both with and without schema resolution.

To run the example:

    $ make
    Writing data...
    Reading data using same schema...
      a: 10, b: 11
      a: 20, b: 21
      a: 30, b: 31
      a: 40, b: 41
      a: 50, b: 51
    Reading data with schema resolution, keeping field "a"...
      a: 10
      a: 20
      a: 30
      a: 40
      a: 50
    Reading data with schema resolution, keeping field "b"...
      b: 11
      b: 21
      b: 31
      b: 41
      b: 51

This directory also containes a [nicely formatted version][formatted] of the
source code.

[formatted]: http://dcreager.github.com/avro-examples/resolved-writer.html

If you update the source code for some reason, you can regenerate the formatted
version if if you have [Rocco][rocco] installed:

    $ make doc
    rocco: resolved-writer.c -> resolved-writer.html

[rocco]: http://rtomayko.github.com/rocco/
