// This example shows how to write data into an Avro container file, and how to
// read data from a file, both with and without schema resolution.  The source
// of this example can be found [on GitHub][gh].
//
// [gh]: https://github.com/dcreager/avro-examples/tree/master/resolved-writer

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <avro.h>

// ### Preliminaries

// These macros help us check for Avro errors.  If any occur, we print out an
// error message and abort the process.

#define check_i(call) \
    do { \
        if ((call) != 0) { \
            fprintf(stderr, "Error: %s\n", avro_strerror()); \
            exit(EXIT_FAILURE); \
        } \
    } while (0)

#define check_p(call) \
    do { \
        if ((call) == NULL) { \
            fprintf(stderr, "Error: %s\n", avro_strerror()); \
            exit(EXIT_FAILURE); \
        } \
    } while (0)

// ### Schemas

// These are the schemas that we'll use to write and read the data.

#define WRITER_SCHEMA \
    "{" \
    "  \"type\": \"record\"," \
    "  \"name\": \"test\"," \
    "  \"fields\": [" \
    "    { \"name\": \"a\", \"type\": \"int\" }," \
    "    { \"name\": \"b\", \"type\": \"int\" }" \
    "  ]" \
    "}"

#define READER_SCHEMA_A \
    "{" \
    "  \"type\": \"record\"," \
    "  \"name\": \"test\"," \
    "  \"fields\": [" \
    "    { \"name\": \"a\", \"type\": \"int\" }" \
    "  ]" \
    "}"

#define READER_SCHEMA_B \
    "{" \
    "  \"type\": \"record\"," \
    "  \"name\": \"test\"," \
    "  \"fields\": [" \
    "    { \"name\": \"b\", \"type\": \"int\" }" \
    "  ]" \
    "}"

// ### Writing data

// This function writes a sequence of integers into a new Avro data file, using
// the `WRITER_SCHEMA`.

static void
write_data(const char *filename)
{
    avro_file_writer_t  file;
    avro_schema_t  writer_schema;
    avro_schema_error_t  error;
    avro_value_iface_t  *writer_iface;
    avro_value_t  writer_value;
    avro_value_t  field;

    // First parse the JSON schema into the C API's internal schema
    // representation.
    check_i(avro_schema_from_json(WRITER_SCHEMA, 0, &writer_schema, &error));

    // Then create a value that is an instance of that schema.  We use the
    // built-in "generic" value implementation, which is what you'll usually use
    // to create value instances that can actually store data.  We only need to
    // create one instance, since we can re-use it for all of the values that
    // we're going to write into the file.
    check_p(writer_iface = avro_generic_class_from_schema(writer_schema));
    check_i(avro_generic_value_new(writer_iface, &writer_value));

    // Open a new data file for writing, and then write a slew of records into
    // it.
    check_i(avro_file_writer_create(filename, writer_schema, &file));

    /* record 1 */
    check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
    check_i(avro_value_set_int(&field, 10));
    check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
    check_i(avro_value_set_int(&field, 11));
    check_i(avro_file_writer_append_value(file, &writer_value));

    /* record 2 */
    check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
    check_i(avro_value_set_int(&field, 20));
    check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
    check_i(avro_value_set_int(&field, 21));
    check_i(avro_file_writer_append_value(file, &writer_value));

    /* record 3 */
    check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
    check_i(avro_value_set_int(&field, 30));
    check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
    check_i(avro_value_set_int(&field, 31));
    check_i(avro_file_writer_append_value(file, &writer_value));

    /* record 4 */
    check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
    check_i(avro_value_set_int(&field, 40));
    check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
    check_i(avro_value_set_int(&field, 41));
    check_i(avro_file_writer_append_value(file, &writer_value));

    /* record 5 */
    check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
    check_i(avro_value_set_int(&field, 50));
    check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
    check_i(avro_value_set_int(&field, 51));
    check_i(avro_file_writer_append_value(file, &writer_value));

    // Close the file and clean up after ourselves.
    avro_file_writer_close(file);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(writer_iface);
    avro_schema_decref(writer_schema);
}


// ### Reading using the actual writer schema

// In this example, we read data from a file, and use the actual writer schema
// when we create the value instance to read into.  We're being a little bit
// loosy-goosy here, because we're assuming that the writer schema is
// `WRITER_SCHEMA`, and that there are `int` fields named `a` and `b` that we
// can grab.  If we were being *really* well-behaved, we'd dynamically
// interrogate the writer schema to see what fields are available.

static void
read_using_writer_schema(const char *filename)
{
    avro_file_reader_t  file;
    avro_schema_t  writer_schema;
    avro_value_iface_t  *writer_iface;
    avro_value_t  writer_value;

    // Open an Avro file and grab the writer schema that was used to create the
    // file.
    check_i(avro_file_reader(filename, &file));
    writer_schema = avro_file_reader_get_writer_schema(file);

    // Then create a value that is an instance of the writer schema.  As above,
    // we use the built-in "generic" value implementation for the value instance
    // that will actually store the data.
    check_p(writer_iface = avro_generic_class_from_schema(writer_schema));
    check_i(avro_generic_value_new(writer_iface, &writer_value));

    // Read values from the file until we run out, printing the contents of each
    // one.  Here, we can read directly into `writer_value` since we know that
    // it's an instance of the schema that was used to create the file.
    while (avro_file_reader_read_value(file, &writer_value) == 0) {
        avro_value_t  field;
        int32_t  a;
        int32_t  b;

        check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
        check_i(avro_value_get_int(&field, &a));
        check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
        check_i(avro_value_get_int(&field, &b));
        printf("  a: %" PRId32 ", b: %" PRId32 "\n", a, b);
    }

    // Close the file and clean up after ourselves.
    avro_file_reader_close(file);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(writer_iface);
    avro_schema_decref(writer_schema);
}


// ### Schema resolution

// In this example, we read from the same data file, but using schema resolution
// to project away all but one of the original fields.  The function lets you
// pass in the reader schema, and the name of the field that's included in the
// reader schema.  That lets us test the projection on both fields without quite
// so much copy-pasta.

static void
read_with_schema_resolution_writer(const char *filename,
                                   const char *reader_schema_json,
                                   const char *field_name)
{
    avro_file_reader_t  file;
    avro_schema_error_t  error;
    avro_schema_t  reader_schema;
    avro_schema_t  writer_schema;
    avro_value_iface_t  *writer_iface;
    avro_value_iface_t  *reader_iface;
    avro_value_t  writer_value;
    avro_value_t  reader_value;

    // Open an Avro file and grab the writer schema that was used to create the
    // file.
    check_i(avro_file_reader(filename, &file));
    writer_schema = avro_file_reader_get_writer_schema(file);

    // Create a value instance that we want to read the data into.  Note that
    // this is *not* the writer schema!
    check_i(avro_schema_from_json
            (reader_schema_json, 0, &reader_schema, &error));
    check_p(reader_iface = avro_generic_class_from_schema(reader_schema));
    check_i(avro_generic_value_new(reader_iface, &reader_value));

    // Create a resolved writer that will perform the schema resolution for us.
    // If the two schemas aren't compatible, this function will return an error,
    // and the error text should describe which parts of the schemas are
    // incompatible.
    check_p(writer_iface =
            avro_resolved_writer_new(writer_schema, reader_schema));

    // Create an instance of the resolved writer, and tell it to wrap our reader
    // value instance.
    check_i(avro_resolved_writer_new_value(writer_iface, &writer_value));
    avro_resolved_writer_set_dest(&writer_value, &reader_value);

    // Now we've got the same basic loop as above.  But we've got two value
    // instances floating around!  Which do we use?  We have the file reader
    // fill in `writer_value`, since that's the value that is an instance of the
    // file's writer schema.  Since it's an instance of a resolved writer,
    // though, it doesn't actually store any data itself.  Instead, it will
    // perform schema resolution on the data read from the file, and fill in its
    // wrapped value (which in our case is `reader_value`).  That means that
    // once the data has been read, we can get its (schema-resolved) contents
    // via `reader_value`.
    while (avro_file_reader_read_value(file, &writer_value) == 0) {
        avro_value_t  field;
        int32_t  value;

        check_i(avro_value_get_by_name(&reader_value, field_name, &field, NULL));
        check_i(avro_value_get_int(&field, &value));
        printf("  %s: %" PRId32 "\n", field_name, value);
    }

    // Close the file and clean up after ourselves.
    avro_file_reader_close(file);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(writer_iface);
    avro_schema_decref(writer_schema);
    avro_value_decref(&reader_value);
    avro_value_iface_decref(reader_iface);
    avro_schema_decref(reader_schema);
}

static void
read_with_schema_resolution_reader(const char *filename,
                            const char *reader_schema_json,
                            const char *field_name)
{
    avro_file_reader_t  file;
    avro_schema_error_t  error;
    avro_schema_t  reader_schema;
    avro_schema_t  writer_schema;
    avro_value_iface_t  *writer_iface;
    avro_value_iface_t  *reader_iface;
    avro_value_t  writer_value;
    avro_value_t  reader_value;

    // Open an Avro file and grab the writer schema that was used to create the
    // file.
    check_i(avro_file_reader(filename, &file));
    writer_schema = avro_file_reader_get_writer_schema(file);

    // Create a value instance that we want to read the data into.  Note that
    // this is *not* the writer schema!
    check_i(avro_schema_from_json
            (reader_schema_json, 0, &reader_schema, &error));
    check_p(writer_iface = avro_generic_class_from_schema(writer_schema));
    check_i(avro_generic_value_new(writer_iface, &writer_value));

    // Create a resolved reader that will perform the schema resolution for us.
    // If the two schemas aren't compatible, this function will return an error,
    // and the error text should describe which parts of the schemas are
    // incompatible.
    check_p(reader_iface =
            avro_resolved_reader_new(writer_schema, reader_schema));

    // Create an instance of the resolved reader, and tell it to wrap our
    // value instance.
    check_i(avro_resolved_reader_new_value(reader_iface, &reader_value));
    avro_resolved_reader_set_source(&reader_value, &writer_value);

    // Now we've got the same basic loop as above.
    while (avro_file_reader_read_value(file, &writer_value) == 0) {
        avro_value_t  field;
        int32_t  value;

        check_i(avro_value_get_by_name(&reader_value, field_name, &field, NULL));
        check_i(avro_value_get_int(&field, &value));
        printf("  %s: %" PRId32 "\n", field_name, value);
    }

    // Close the file and clean up after ourselves.
    avro_file_reader_close(file);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(reader_iface);
    avro_schema_decref(writer_schema);
    avro_value_decref(&reader_value);
    avro_value_iface_decref(reader_iface);
    avro_schema_decref(reader_schema);
}


// ### Postliminaries?

// And finally the function that gets this party started.

int
main(void)
{
#define FILENAME  "test-data.avro"

    printf("Writing data...\n");
    write_data(FILENAME);

    printf("Reading data using same schema...\n");
    read_using_writer_schema(FILENAME);

    printf("Reading data with schema resolution using writer, keeping field \"a\"...\n");
    read_with_schema_resolution_writer(FILENAME, READER_SCHEMA_A, "a");

    printf("Reading data with schema resolution using writer, keeping field \"b\"...\n");
    read_with_schema_resolution_writer(FILENAME, READER_SCHEMA_B, "b");

    printf("Reading data with schema resolution using reader, keeping field \"a\"...\n");
    read_with_schema_resolution_reader(FILENAME, READER_SCHEMA_A, "a");

    printf("Reading data with schema resolution using reader, keeping field \"b\"...\n");
    read_with_schema_resolution_reader(FILENAME, READER_SCHEMA_B, "b");

    return 0;
}
