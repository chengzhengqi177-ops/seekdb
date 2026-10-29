/**
 * Dependency-free Parquet reader for the seekdb "file as database" feature.
 *
 * The implementation lives in file_sql_parquet.cpp and uses only the C++
 * standard library: a hand-written Thrift Compact Protocol decoder, a raw
 * Snappy block decompressor and the Parquet page decoders are all contained
 * there.
 *
 * Supported subset (see file_sql_parquet.cpp for details):
 *   - flat schemas (one root element + primitive leaf columns)
 *   - REQUIRED / OPTIONAL leaves (definition levels, max level 1)
 *   - BOOLEAN / INT32 / INT64 / FLOAT / DOUBLE / BYTE_ARRAY /
 *     FIXED_LEN_BYTE_ARRAY physical types
 *   - PLAIN and PLAIN_DICTIONARY / RLE_DICTIONARY encodings
 *   - UNCOMPRESSED and SNAPPY compression
 *   - multiple row groups and multiple pages per column chunk
 *
 * Anything else (INT96, REPEATED / nested columns, GZIP/ZSTD/LZ4/BROTLI,
 * delta and byte-stream-split encodings) is rejected with a descriptive
 * error message instead of crashing.
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "file_sql_types.h"

namespace oceanbase {
namespace sql {
namespace filesql {

// Creates a reader over all columns of the parquet file. The default breadth
// (all leaf columns, top to bottom) is used when no projection is supplied.
std::unique_ptr<IFileReader> create_parquet_reader();

// Creates a reader that only materialises the columns named in `projection`
// (in that order; unknown names are reported by open()). An empty projection
// is equivalent to create_parquet_reader().
std::unique_ptr<IFileReader> create_parquet_reader(const std::vector<std::string> &projection);

// Returns the last error message recorded by a reader created above, or an
// empty string when `reader` is not a Parquet reader. This is needed because
// IFileReader::open() cannot carry an error string; after a failed open() the
// same message is also returned by next_row()'s `err` argument.
std::string parquet_reader_last_error(const IFileReader *reader);

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
