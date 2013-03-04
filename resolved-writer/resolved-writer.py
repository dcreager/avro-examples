#!/usr/bin/env python

'''
This example shows how to write data into an Avro container file, and how to
read data from a container file, both with and without schema resolution.

It is modelled on the resolver-writer.c example file.
'''

from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


WRITER_SCHEMA = """{
 "type": "record",
 "name": "test",
 "fields": [
   {"name": "a", "type": "int"},
   {"name": "b", "type": "int"}]}
"""

READER_SCHEMA_A = """{
 "type": "record",
 "name": "test",
 "fields": [
   {"name": "a", "type": "int"}]}
"""

READER_SCHEMA_B = """{
 "type": "record",
 "name": "test",
 "fields": [
   {"name": "b", "type": "int"}]}
"""

READER_SCHEMA_C = """{
 "type": "record",
 "name": "test",
 "fields": [
   {"name": "a", "type": "int"},
   {"name": "b", "type": "int"},
   {"name": "c", "type": ["int", "null"], "default": 42}]}
"""


#
# This function writes a sequence of integers into a new Avro data file,
# using the `WRITER_SCHEMA`.
#
def write_data(filename):
    '''  '''

    with open(filename, "wb") as file_fd:

        writer_schema = schema.parse(WRITER_SCHEMA)

        writer = DataFileWriter(file_fd,
                                DatumWriter(),
                                writer_schema,
                                codec='deflate')

        # create 5 entries in the data file
        for i in range(1, 6):
            a_val = i * 10
            b_val = i * 10 + 1
            item = {"a": a_val, "b": b_val}
            writer.append(item)

        writer.close()


def read_using_writer_schema(filename):
    ''' Read data file contents using writer schema in data file '''

    with open(filename, "rb") as file_fd:

        reader = DataFileReader(file_fd, DatumReader())

        for datum in reader:
            print "a: %i, b: %i" % (datum['a'], datum['b'])

        reader.close()


def read_with_schema_resolution(filename, reader_schema_json, field_name):
    ''' Read data file contents by resolving to reader schema '''

    with open(filename, "rb") as file_fd:

        reader_schema = schema.parse(reader_schema_json)

        datum_reader = DatumReader(readers_schema=reader_schema)

        reader = DataFileReader(file_fd, datum_reader)

        for datum in reader:
            print "%s: %i" % (field_name, datum[field_name])

        reader.close()


if __name__ == "__main__":

    FILENAME = "test-data.avro"

    print "Writing data..."
    write_data(FILENAME)

    print "Reading data using writer schema in datafile..."
    read_using_writer_schema(FILENAME)

    print "Reading data with schema resolution, keeping field 'a'..."
    read_with_schema_resolution(FILENAME, READER_SCHEMA_A, "a")

    print "Reading data with schema resolution, keeping field 'b'..."
    read_with_schema_resolution(FILENAME, READER_SCHEMA_B, "b")

    print "Reading data with evolved schema resolution, showing new field 'c'..."
    read_with_schema_resolution(FILENAME, READER_SCHEMA_C, "c")
