/**
 * Dependency-free Parquet reader for the seekdb "file as database" feature.
 *
 * Everything below is implemented from the Parquet/Thrift/Snappy format
 * specifications using only the C++ standard library:
 *
 *   * Thrift Compact Protocol decoder (field headers, zigzag varints,
 *     binary/list/map/struct, boolean-in-field-header)
 *   * Parquet metadata structures (FileMetaData, SchemaElement, RowGroup,
 *     ColumnChunk, ColumnMetaData, PageHeader, DataPageHeader,
 *     DataPageHeaderV2, DictionaryPageHeader)
 *   * page decoders for PLAIN data with definition levels and for
 *     PLAIN_DICTIONARY / RLE_DICTIONARY data (RLE/bit-packed hybrid)
 *   * a raw Snappy block decompressor for SNAPPY compressed pages
 *
 * The reader is column projected and page streaming: only the column chunks
 * selected by the projection are touched and every column chunk is decoded
 * page by page (never the whole file at once).
 *
 * Unsupported features are rejected with descriptive errors rather than
 * undefined behaviour:
 *   - INT96 timestamps, nested / REPEATED columns
 *   - GZIP / LZO / BROTLI / LZ4 / ZSTD compression
 *   - DELTA_* / BYTE_STREAM_SPLIT / plain-RLE value encodings
 */

#include "file_sql_parquet.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <utility>

namespace oceanbase {
namespace sql {
namespace filesql {
namespace {

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

typedef std::int64_t i64;
typedef std::int32_t i32;
typedef std::uint8_t u8;
typedef std::uint32_t u32;
typedef std::uint64_t u64;

const i64 kParquetMagicLen = 4;
const size_t kFileBufSize = 64 * 1024;

bool set_error(std::string &err, const std::string &msg) {
  err = msg;
  return false;
}

std::string int_to_string(i64 v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(v));
  return std::string(buf);
}

u32 load_le32(const u8 *p) {
  return static_cast<u32>(p[0]) | (static_cast<u32>(p[1]) << 8) |
         (static_cast<u32>(p[2]) << 16) | (static_cast<u32>(p[3]) << 24);
}

u64 load_le64(const u8 *p) {
  u64 v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= static_cast<u64>(p[i]) << (8 * i);
  }
  return v;
}

bool load_le_varint(const u8 *data, size_t size, size_t &pos, u64 &out) {
  out = 0;
  int shift = 0;
  while (pos < size) {
    u8 b = data[pos++];
    out |= static_cast<u64>(b & 0x7f) << shift;
    if ((b & 0x80) == 0) {
      return true;
    }
    shift += 7;
    if (shift > 63) {
      return false;
    }
  }
  return false;
}

const char *physical_type_name(int type) {
  switch (type) {
    case 0: return "BOOLEAN";
    case 1: return "INT32";
    case 2: return "INT64";
    case 3: return "INT96";
    case 4: return "FLOAT";
    case 5: return "DOUBLE";
    case 6: return "BYTE_ARRAY";
    case 7: return "FIXED_LEN_BYTE_ARRAY";
    default: return "UNKNOWN";
  }
}

const char *codec_name(int codec) {
  switch (codec) {
    case 0: return "UNCOMPRESSED";
    case 1: return "SNAPPY";
    case 2: return "GZIP";
    case 3: return "LZO";
    case 4: return "BROTLI";
    case 5: return "LZ4";
    case 6: return "ZSTD";
    case 7: return "LZ4_RAW";
    default: return "UNKNOWN";
  }
}

const char *encoding_name(int encoding) {
  switch (encoding) {
    case 0: return "PLAIN";
    case 2: return "PLAIN_DICTIONARY";
    case 3: return "RLE";
    case 4: return "BIT_PACKED";
    case 5: return "DELTA_BINARY_PACKED";
    case 6: return "DELTA_LENGTH_BYTE_ARRAY";
    case 7: return "DELTA_BYTE_ARRAY";
    case 8: return "RLE_DICTIONARY";
    case 9: return "BYTE_STREAM_SPLIT";
    default: return "UNKNOWN";
  }
}

// Converts "days since 1970-01-01" to "YYYY-MM-DD" (proleptic Gregorian).
void format_days_as_date(i32 days, std::string &out) {
  i64 z = static_cast<i64>(days) + 719468;
  i64 era = (z >= 0 ? z : z - 146096) / 146097;
  u64 doe = static_cast<u64>(z - era * 146097);
  u64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  i64 y = static_cast<i64>(yoe) + era * 400;
  u64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  u64 mp = (5 * doy + 2) / 153;
  u64 d = doy - (153 * mp + 2) / 5 + 1;
  u64 m = mp < 10 ? mp + 3 : mp - 9;
  if (m <= 2) {
    ++y;
  }
  char buf[40];
  std::snprintf(buf, sizeof(buf), "%04lld-%02llu-%02llu",
                static_cast<long long>(y), static_cast<unsigned long long>(m),
                static_cast<unsigned long long>(d));
  out.assign(buf);
}

// ---------------------------------------------------------------------------
// Buffered sequential file reader
// ---------------------------------------------------------------------------

class FileStream {
 public:
  FileStream() : size_(0), limit_(0), buf_start_(0), buf_pos_(0), buf_len_(0) {
    buf_.resize(kFileBufSize);
  }
  ~FileStream() { close(); }

  bool open(const std::string &path, std::string &err) {
    close();
    in_.open(path.c_str(), std::ios::in | std::ios::binary);
    if (!in_.is_open()) {
      return set_error(err, "cannot open parquet file '" + path + "'");
    }
    in_.seekg(0, std::ios::end);
    std::streamoff sz = in_.tellg();
    if (!in_ || sz < 0) {
      return set_error(err, "cannot determine size of parquet file '" + path + "'");
    }
    size_ = static_cast<i64>(sz);
    limit_ = size_;
    buf_start_ = 0;
    buf_pos_ = 0;
    buf_len_ = 0;
    in_.clear();
    in_.seekg(0, std::ios::beg);
    return true;
  }

  void close() {
    if (in_.is_open()) {
      in_.close();
    }
    buf_start_ = 0;
    buf_pos_ = 0;
    buf_len_ = 0;
    limit_ = 0;
  }

  bool is_open() const { return in_.is_open(); }
  i64 size() const { return size_; }
  void set_limit(i64 lim) { limit_ = lim; }
  i64 remaining() const { return limit_ - pos(); }
  i64 pos() const { return buf_start_ + static_cast<i64>(buf_pos_); }

  bool seek(i64 off) {
    if (off < 0 || off > size_) {
      return false;
    }
    if (off >= buf_start_ && off <= buf_start_ + static_cast<i64>(buf_len_)) {
      buf_pos_ = static_cast<size_t>(off - buf_start_);
      return true;
    }
    in_.clear();
    in_.seekg(static_cast<std::streamoff>(off), std::ios::beg);
    if (!in_) {
      return false;
    }
    buf_start_ = off;
    buf_pos_ = 0;
    buf_len_ = 0;
    return true;
  }

  bool read_byte(u8 &b) {
    if (pos() >= limit_) {
      return false;
    }
    if (buf_pos_ >= buf_len_ && !refill()) {
      return false;
    }
    b = buf_[buf_pos_++];
    return true;
  }

  bool read_bytes(void *dst, size_t n) {
    u8 *out = static_cast<u8 *>(dst);
    while (n > 0) {
      if (pos() >= limit_) {
        return false;
      }
      if (buf_pos_ >= buf_len_ && !refill()) {
        return false;
      }
      size_t avail = buf_len_ - buf_pos_;
      i64 remain = limit_ - pos();
      if (remain < static_cast<i64>(avail)) {
        avail = static_cast<size_t>(remain);
      }
      size_t take = n < avail ? n : avail;
      if (take == 0) {
        return false;
      }
      std::memcpy(out, &buf_[buf_pos_], take);
      out += take;
      buf_pos_ += take;
      n -= take;
    }
    return true;
  }

  bool skip_bytes(i64 n) {
    if (n < 0) {
      return false;
    }
    i64 target = pos() + n;
    if (target < pos() || target > limit_) {
      return false;
    }
    if (target >= buf_start_ && target <= buf_start_ + static_cast<i64>(buf_len_)) {
      buf_pos_ = static_cast<size_t>(target - buf_start_);
      return true;
    }
    return seek(target);
  }

 private:
  bool refill() {
    buf_start_ = pos();
    in_.read(reinterpret_cast<char *>(&buf_[0]), static_cast<std::streamsize>(buf_.size()));
    std::streamsize got = in_.gcount();
    if (got <= 0) {
      buf_pos_ = 0;
      buf_len_ = 0;
      return false;
    }
    buf_pos_ = 0;
    buf_len_ = static_cast<size_t>(got);
    return true;
  }

  std::ifstream in_;
  std::vector<u8> buf_;
  i64 size_;
  i64 limit_;
  i64 buf_start_;
  size_t buf_pos_;
  size_t buf_len_;
};

// ---------------------------------------------------------------------------
// Thrift Compact Protocol decoder
// ---------------------------------------------------------------------------

enum CompactType {
  CT_STOP = 0,
  CT_BOOL_TRUE = 1,
  CT_BOOL_FALSE = 2,
  CT_BYTE = 3,
  CT_I16 = 4,
  CT_I32 = 5,
  CT_I64 = 6,
  CT_DOUBLE = 7,
  CT_BINARY = 8,
  CT_LIST = 9,
  CT_SET = 10,
  CT_MAP = 11,
  CT_STRUCT = 12,
};

class ThriftReader {
 public:
  explicit ThriftReader(FileStream &s) : stream_(s), failed_(false) {}

  const std::string &error() const { return err_; }
  bool failed() const { return failed_; }

  bool fail(const std::string &msg) {
    if (!failed_) {
      failed_ = true;
      err_ = msg;
    }
    return false;
  }

  bool begin_struct() {
    if (failed_) {
      return false;
    }
    if (struct_stack_.size() >= 128) {
      return fail("thrift nesting too deep");
    }
    struct_stack_.push_back(0);
    return true;
  }

  // Returns true while a field header was read; false on STOP or error.
  bool next_field(int &field_id, int &type) {
    if (failed_) {
      return false;
    }
    if (struct_stack_.empty()) {
      return fail("thrift field outside of a struct");
    }
    u8 b = 0;
    if (!stream_.read_byte(b)) {
      return fail("unexpected end of file while reading a thrift field header");
    }
    if (b == 0) {
      struct_stack_.pop_back();
      return false;
    }
    int delta = b >> 4;
    type = b & 0x0f;
    if (type > CT_STRUCT) {
      return fail("invalid thrift field type " + int_to_string(type));
    }
    int id = 0;
    if (delta == 0) {
      i64 z = 0;
      if (!read_zigzag(z)) {
        return false;
      }
      id = static_cast<int>(z);
    } else {
      id = struct_stack_.back() + delta;
    }
    if (id <= 0) {
      return fail("invalid thrift field id " + int_to_string(id));
    }
    struct_stack_.back() = id;
    field_id = id;
    return true;
  }

  bool skip(int type) {
    switch (type) {
      case CT_BOOL_TRUE:
      case CT_BOOL_FALSE:
        return !failed_;
      case CT_BYTE: {
        u8 b = 0;
        if (!stream_.read_byte(b)) {
          return fail("unexpected end of file while skipping a byte field");
        }
        return true;
      }
      case CT_I16:
      case CT_I32:
      case CT_I64: {
        i64 z = 0;
        return read_zigzag(z);
      }
      case CT_DOUBLE: {
        u8 b[8];
        if (!stream_.read_bytes(b, 8)) {
          return fail("unexpected end of file while skipping a double field");
        }
        return true;
      }
      case CT_BINARY: {
        std::string tmp;
        return read_binary(tmp);
      }
      case CT_LIST:
      case CT_SET: {
        int etype = 0;
        i64 n = 0;
        if (!read_list_header(etype, n)) {
          return false;
        }
        for (i64 i = 0; i < n; ++i) {
          if (!skip(etype)) {
            return false;
          }
        }
        return true;
      }
      case CT_MAP: {
        u64 n = 0;
        if (!read_uvarint(n)) {
          return false;
        }
        if (n > 0) {
          u8 h = 0;
          if (!stream_.read_byte(h)) {
            return fail("unexpected end of file while reading a map header");
          }
          int kt = h >> 4;
          int vt = h & 0x0f;
          for (u64 i = 0; i < n; ++i) {
            if (!skip(kt) || !skip(vt)) {
              return false;
            }
          }
        }
        return true;
      }
      case CT_STRUCT: {
        if (!begin_struct()) {
          return false;
        }
        int fid = 0;
        int ft = 0;
        while (next_field(fid, ft)) {
          if (!skip(ft)) {
            return false;
          }
        }
        return !failed_;
      }
      default:
        return fail("cannot skip unknown thrift type " + int_to_string(type));
    }
  }

  bool read_bool(int type, bool &out) {
    if (type == CT_BOOL_TRUE) {
      out = true;
      return true;
    }
    if (type == CT_BOOL_FALSE) {
      out = false;
      return true;
    }
    return fail("expected a thrift bool field, got type " + int_to_string(type));
  }

  // Integer fields: tolerant of i16/i32/i64 declarations on the wire.
  bool read_int(int type, i64 &out) {
    if (type != CT_I16 && type != CT_I32 && type != CT_I64) {
      return fail("expected an integer thrift field, got type " + int_to_string(type));
    }
    return read_zigzag(out);
  }

  bool read_i32(int type, i64 &out) {
    if (!read_int(type, out)) {
      return false;
    }
    if (out < std::numeric_limits<i32>::min() || out > std::numeric_limits<i32>::max()) {
      return fail("thrift i32 value out of range");
    }
    return true;
  }

  bool read_i32(int type, int &out) {
    i64 v = 0;
    if (!read_i32(type, v)) {
      return false;
    }
    out = static_cast<int>(v);
    return true;
  }

  bool read_double(int type, double &out) {
    if (type != CT_DOUBLE) {
      return fail("expected a thrift double field, got type " + int_to_string(type));
    }
    u8 b[8];
    if (!stream_.read_bytes(b, 8)) {
      return fail("unexpected end of file while reading a double");
    }
    u64 v = load_le64(b);
    std::memcpy(&out, &v, sizeof(out));
    return true;
  }

  bool read_binary(std::string &out) {
    u64 len = 0;
    if (!read_uvarint(len)) {
      return false;
    }
    if (len > static_cast<u64>(stream_.remaining())) {
      return fail("thrift binary length exceeds remaining file bytes");
    }
    out.resize(static_cast<size_t>(len));
    if (len > 0 && !stream_.read_bytes(&out[0], static_cast<size_t>(len))) {
      return fail("unexpected end of file while reading a binary field");
    }
    return true;
  }

  bool read_list_header(int &elem_type, i64 &size) {
    u8 b = 0;
    if (!stream_.read_byte(b)) {
      return fail("unexpected end of file while reading a list header");
    }
    size = b >> 4;
    elem_type = b & 0x0f;
    if (size == 15) {
      u64 v = 0;
      if (!read_uvarint(v)) {
        return false;
      }
      size = static_cast<i64>(v);
    }
    if (size < 0 || elem_type > CT_STRUCT) {
      return fail("malformed thrift list header");
    }
    if (size > static_cast<i64>(stream_.remaining()) + 64) {
      return fail("thrift list size exceeds remaining file bytes");
    }
    return true;
  }

 private:
  bool read_uvarint(u64 &out) {
    out = 0;
    int shift = 0;
    for (;;) {
      u8 b = 0;
      if (!stream_.read_byte(b)) {
        return fail("unexpected end of file while reading a varint");
      }
      out |= static_cast<u64>(b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        return true;
      }
      shift += 7;
      if (shift > 63) {
        return fail("thrift varint is too long");
      }
    }
  }

  bool read_zigzag(i64 &out) {
    u64 n = 0;
    if (!read_uvarint(n)) {
      return false;
    }
    out = static_cast<i64>((n >> 1) ^ (~(n & 1) + 1));
    return true;
  }

  FileStream &stream_;
  std::vector<int> struct_stack_;
  bool failed_;
  std::string err_;
};

// ---------------------------------------------------------------------------
// Parquet metadata
// ---------------------------------------------------------------------------

enum PhysicalType {
  PT_BOOLEAN = 0,
  PT_INT32 = 1,
  PT_INT64 = 2,
  PT_INT96 = 3,
  PT_FLOAT = 4,
  PT_DOUBLE = 5,
  PT_BYTE_ARRAY = 6,
  PT_FIXED_LEN_BYTE_ARRAY = 7,
};

enum RepetitionType {
  REP_REQUIRED = 0,
  REP_OPTIONAL = 1,
  REP_REPEATED = 2,
};

enum ParquetPageType {
  PAGE_DATA = 0,
  PAGE_INDEX = 1,
  PAGE_DICTIONARY = 2,
  PAGE_DATA_V2 = 3,
};

enum ParquetEncoding {
  ENC_PLAIN = 0,
  ENC_PLAIN_DICTIONARY = 2,
  ENC_RLE = 3,
  ENC_BIT_PACKED = 4,
  ENC_DELTA_BINARY_PACKED = 5,
  ENC_DELTA_LENGTH_BYTE_ARRAY = 6,
  ENC_DELTA_BYTE_ARRAY = 7,
  ENC_RLE_DICTIONARY = 8,
  ENC_BYTE_STREAM_SPLIT = 9,
};

enum ParquetCodec {
  CODEC_UNCOMPRESSED = 0,
  CODEC_SNAPPY = 1,
  CODEC_GZIP = 2,
  CODEC_LZO = 3,
  CODEC_BROTLI = 4,
  CODEC_LZ4 = 5,
  CODEC_ZSTD = 6,
  CODEC_LZ4_RAW = 7,
};

struct SchemaElement {
  int type = -1;
  i64 type_length = 0;
  int repetition = -1;
  std::string name;
  i64 num_children = -1;
  int converted_type = -1;
  // 0: none/ignored, 1: UTF8/STRING, 2: DATE
  int annotation = 0;
};

struct ColumnMetaData {
  int type = -1;
  std::vector<int> encodings;
  std::vector<std::string> path;
  int codec = -1;
  i64 num_values = 0;
  i64 total_uncompressed_size = 0;
  i64 total_compressed_size = 0;
  i64 data_page_offset = 0;
  i64 index_page_offset = 0;
  i64 dictionary_page_offset = 0;
};

struct ColumnChunk {
  std::string file_path;
  i64 file_offset = 0;
  bool has_meta = false;
  ColumnMetaData meta;
};

struct RowGroup {
  std::vector<ColumnChunk> columns;
  i64 num_rows = 0;
  i64 total_byte_size = 0;
  i64 total_compressed_size = 0;
};

struct FileMetaData {
  i64 version = 0;
  std::vector<SchemaElement> schema;
  i64 num_rows = 0;
  std::vector<RowGroup> row_groups;
  std::string created_by;
};

bool tr_error(ThriftReader &tr, std::string &err, const char *what) {
  err.assign(what);
  if (!tr.error().empty()) {
    err += ": ";
    err += tr.error();
  }
  return false;
}

bool parse_logical_type(ThriftReader &tr, int &annotation, std::string &err) {
  annotation = 0;
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "LogicalType");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    if (fid == 1) {  // StringType
      annotation = 1;
      if (!tr.skip(ft)) {
        return tr_error(tr, err, "LogicalType.StringType");
      }
    } else if (fid == 6) {  // DateType
      annotation = 2;
      if (!tr.skip(ft)) {
        return tr_error(tr, err, "LogicalType.DateType");
      }
    } else if (!tr.skip(ft)) {
      return tr_error(tr, err, "LogicalType");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "LogicalType");
  }
  return true;
}

bool parse_schema_element(ThriftReader &tr, SchemaElement &e, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "SchemaElement");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_i32(ft, e.type); break;
      case 2: ok = tr.read_int(ft, e.type_length); break;
      case 3: ok = tr.read_i32(ft, e.repetition); break;
      case 4: ok = tr.read_binary(e.name); break;
      case 5: ok = tr.read_int(ft, e.num_children); break;
      case 6: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        e.converted_type = static_cast<int>(v);
        break;
      }
      case 10: ok = parse_logical_type(tr, e.annotation, err); break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "SchemaElement");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "SchemaElement");
  }
  return true;
}

bool parse_column_metadata(ThriftReader &tr, ColumnMetaData &m, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "ColumnMetaData");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_i32(ft, m.type); break;
      case 2: {
        int etype = 0;
        i64 n = 0;
        ok = tr.read_list_header(etype, n);
        for (i64 i = 0; ok && i < n; ++i) {
          i64 v = 0;
          ok = tr.read_int(etype, v);
          if (ok) {
            m.encodings.push_back(static_cast<int>(v));
          }
        }
        break;
      }
      case 3: {
        int etype = 0;
        i64 n = 0;
        ok = tr.read_list_header(etype, n);
        for (i64 i = 0; ok && i < n; ++i) {
          std::string s;
          ok = tr.read_binary(s);
          if (ok) {
            m.path.push_back(s);
          }
        }
        break;
      }
      case 4: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        m.codec = static_cast<int>(v);
        break;
      }
      case 5: ok = tr.read_int(ft, m.num_values); break;
      case 6: ok = tr.read_int(ft, m.total_uncompressed_size); break;
      case 7: ok = tr.read_int(ft, m.total_compressed_size); break;
      case 9: ok = tr.read_int(ft, m.data_page_offset); break;
      case 10: ok = tr.read_int(ft, m.index_page_offset); break;
      case 11: ok = tr.read_int(ft, m.dictionary_page_offset); break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "ColumnMetaData");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "ColumnMetaData");
  }
  return true;
}

bool parse_column_chunk(ThriftReader &tr, ColumnChunk &c, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "ColumnChunk");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_binary(c.file_path); break;
      case 2: ok = tr.read_int(ft, c.file_offset); break;
      case 3: ok = parse_column_metadata(tr, c.meta, err); c.has_meta = ok; break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "ColumnChunk");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "ColumnChunk");
  }
  return true;
}

bool parse_row_group(ThriftReader &tr, RowGroup &rg, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "RowGroup");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: {
        int etype = 0;
        i64 n = 0;
        ok = tr.read_list_header(etype, n);
        for (i64 i = 0; ok && i < n; ++i) {
          ColumnChunk c;
          ok = parse_column_chunk(tr, c, err);
          if (ok) {
            rg.columns.push_back(std::move(c));
          }
        }
        break;
      }
      case 2: ok = tr.read_int(ft, rg.total_byte_size); break;
      case 3: ok = tr.read_int(ft, rg.num_rows); break;
      case 6: ok = tr.read_int(ft, rg.total_compressed_size); break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "RowGroup");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "RowGroup");
  }
  return true;
}

bool parse_file_metadata(ThriftReader &tr, FileMetaData &fm, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "FileMetaData");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_i32(ft, fm.version); break;
      case 2: {
        int etype = 0;
        i64 n = 0;
        ok = tr.read_list_header(etype, n);
        for (i64 i = 0; ok && i < n; ++i) {
          SchemaElement e;
          ok = parse_schema_element(tr, e, err);
          if (ok) {
            fm.schema.push_back(std::move(e));
          }
        }
        break;
      }
      case 3: ok = tr.read_int(ft, fm.num_rows); break;
      case 4: {
        int etype = 0;
        i64 n = 0;
        ok = tr.read_list_header(etype, n);
        for (i64 i = 0; ok && i < n; ++i) {
          RowGroup rg;
          ok = parse_row_group(tr, rg, err);
          if (ok) {
            fm.row_groups.push_back(std::move(rg));
          }
        }
        break;
      }
      case 6: ok = tr.read_binary(fm.created_by); break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "FileMetaData");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "FileMetaData");
  }
  return true;
}

// ---------------------------------------------------------------------------
// Page headers
// ---------------------------------------------------------------------------

struct PageHeaderInfo {
  int type = -1;
  i64 uncompressed_page_size = -1;
  i64 compressed_page_size = -1;
  // DataPageHeader / DataPageHeaderV2
  i64 num_values = 0;
  int encoding = -1;
  int definition_level_encoding = -1;
  int repetition_level_encoding = -1;
  i64 num_nulls = 0;
  i64 num_rows = 0;
  i64 definition_levels_byte_length = 0;
  i64 repetition_levels_byte_length = 0;
  bool is_compressed = true;
  // DictionaryPageHeader
  i64 dictionary_num_values = 0;
  int dictionary_encoding = -1;
};

bool parse_data_page_header(ThriftReader &tr, PageHeaderInfo &ph, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "DataPageHeader");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_i32(ft, ph.num_values); break;
      case 2: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        ph.encoding = static_cast<int>(v);
        break;
      }
      case 3: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        ph.definition_level_encoding = static_cast<int>(v);
        break;
      }
      case 4: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        ph.repetition_level_encoding = static_cast<int>(v);
        break;
      }
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "DataPageHeader");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "DataPageHeader");
  }
  return true;
}

bool parse_data_page_header_v2(ThriftReader &tr, PageHeaderInfo &ph, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "DataPageHeaderV2");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_i32(ft, ph.num_values); break;
      case 2: ok = tr.read_i32(ft, ph.num_nulls); break;
      case 3: ok = tr.read_i32(ft, ph.num_rows); break;
      case 4: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        ph.encoding = static_cast<int>(v);
        break;
      }
      case 5: ok = tr.read_i32(ft, ph.definition_levels_byte_length); break;
      case 6: ok = tr.read_i32(ft, ph.repetition_levels_byte_length); break;
      case 7: ok = tr.read_bool(ft, ph.is_compressed); break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "DataPageHeaderV2");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "DataPageHeaderV2");
  }
  return true;
}

bool parse_dictionary_page_header(ThriftReader &tr, PageHeaderInfo &ph, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "DictionaryPageHeader");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: ok = tr.read_i32(ft, ph.dictionary_num_values); break;
      case 2: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        ph.dictionary_encoding = static_cast<int>(v);
        break;
      }
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "DictionaryPageHeader");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "DictionaryPageHeader");
  }
  return true;
}

bool parse_page_header(ThriftReader &tr, PageHeaderInfo &ph, std::string &err) {
  if (!tr.begin_struct()) {
    return tr_error(tr, err, "PageHeader");
  }
  int fid = 0;
  int ft = 0;
  while (tr.next_field(fid, ft)) {
    bool ok = true;
    switch (fid) {
      case 1: {
        i64 v = 0;
        ok = tr.read_i32(ft, v);
        ph.type = static_cast<int>(v);
        break;
      }
      case 2: ok = tr.read_i32(ft, ph.uncompressed_page_size); break;
      case 3: ok = tr.read_i32(ft, ph.compressed_page_size); break;
      case 4: ok = tr.skip(ft); break;  // crc
      case 5: ok = parse_data_page_header(tr, ph, err); break;
      case 7: ok = parse_dictionary_page_header(tr, ph, err); break;
      case 8: ok = parse_data_page_header_v2(tr, ph, err); break;
      default: ok = tr.skip(ft); break;
    }
    if (!ok) {
      return tr_error(tr, err, "PageHeader");
    }
  }
  if (tr.failed()) {
    return tr_error(tr, err, "PageHeader");
  }
  return true;
}

// ---------------------------------------------------------------------------
// Snappy raw block decompressor
// ---------------------------------------------------------------------------

bool snappy_decompress(const u8 *src, size_t size, i64 expected_size,
                       std::vector<u8> &out, std::string &err) {
  out.clear();
  size_t pos = 0;
  u64 declared = 0;
  if (!load_le_varint(src, size, pos, declared)) {
    return set_error(err, "snappy: truncated uncompressed length varint");
  }
  if (declared > static_cast<u64>(std::numeric_limits<i32>::max()) * 4) {
    return set_error(err, "snappy: implausible uncompressed length");
  }
  if (expected_size >= 0 && static_cast<u64>(expected_size) != declared) {
    return set_error(err, "snappy: uncompressed length mismatch (header " +
                              int_to_string(static_cast<i64>(declared)) + ", expected " +
                              int_to_string(expected_size) + ")");
  }
  out.reserve(static_cast<size_t>(declared));
  while (pos < size) {
    u8 tag = src[pos++];
    int kind = tag & 3;
    if (kind == 0) {
      // Literal.
      i64 len = static_cast<i64>(tag >> 2) + 1;
      if (len > 60) {
        int extra = static_cast<int>(len - 60);
        if (static_cast<size_t>(extra) > size - pos) {
          return set_error(err, "snappy: truncated literal length");
        }
        u64 v = 0;
        for (int i = 0; i < extra; ++i) {
          v |= static_cast<u64>(src[pos++]) << (8 * i);
        }
        len = static_cast<i64>(v) + 1;
        if (len <= 0 || len > static_cast<i64>(size - pos)) {
          return set_error(err, "snappy: invalid literal length");
        }
      } else if (len > static_cast<i64>(size - pos)) {
        return set_error(err, "snappy: literal exceeds input");
      }
      if (out.size() + static_cast<size_t>(len) > declared) {
        return set_error(err, "snappy: literal exceeds declared output size");
      }
      out.insert(out.end(), src + pos, src + pos + len);
      pos += static_cast<size_t>(len);
    } else {
      // Copy from already decompressed output.
      i64 len = 0;
      u32 offset = 0;
      if (kind == 1) {
        if (size - pos < 1) {
          return set_error(err, "snappy: truncated 1-byte copy");
        }
        len = static_cast<i64>(((tag >> 2) & 0x07)) + 4;
        offset = (static_cast<u32>(tag >> 5) << 8) | src[pos++];
      } else if (kind == 2) {
        if (size - pos < 2) {
          return set_error(err, "snappy: truncated 2-byte copy");
        }
        len = static_cast<i64>(tag >> 2) + 1;
        offset = load_le32(src + pos) & 0xffff;
        pos += 2;
      } else {
        if (size - pos < 4) {
          return set_error(err, "snappy: truncated 4-byte copy");
        }
        len = static_cast<i64>(tag >> 2) + 1;
        offset = load_le32(src + pos);
        pos += 4;
      }
      if (offset == 0 || static_cast<size_t>(offset) > out.size()) {
        return set_error(err, "snappy: copy offset out of range");
      }
      if (out.size() + static_cast<size_t>(len) > declared) {
        return set_error(err, "snappy: copy exceeds declared output size");
      }
      size_t from = out.size() - offset;
      for (i64 i = 0; i < len; ++i) {
        out.push_back(out[from + static_cast<size_t>(i)]);
      }
    }
  }
  if (out.size() != static_cast<size_t>(declared)) {
    return set_error(err, "snappy: decompressed size mismatch");
  }
  if (expected_size >= 0 && out.size() != static_cast<size_t>(expected_size)) {
    return set_error(err, "snappy: unexpected decompressed page size");
  }
  return true;
}

// ---------------------------------------------------------------------------
// RLE / bit-packed hybrid decoder (levels and dictionary indices)
// ---------------------------------------------------------------------------

class RleDecoder {
 public:
  RleDecoder(const u8 *data, size_t size, int bit_width)
      : data_(data), size_(size), pos_(0), bit_width_(bit_width),
        rle_remaining_(0), rle_value_(0), packed_remaining_(0),
        bit_buf_(0), bit_count_(0) {
    if (bit_width < 0 || bit_width > 32) {
      failed_ = true;
      err_ = "invalid RLE bit width " + int_to_string(bit_width);
    } else {
      failed_ = false;
    }
  }

  bool failed() const { return failed_; }
  const std::string &error() const { return err_; }

  bool next(u32 &value) {
    if (failed_) {
      return false;
    }
    for (;;) {
      if (rle_remaining_ > 0) {
        value = rle_value_;
        --rle_remaining_;
        return true;
      }
      if (packed_remaining_ > 0) {
        if (!take_packed(value)) {
          return false;
        }
        --packed_remaining_;
        return true;
      }
      if (!start_run()) {
        return false;
      }
    }
  }

  bool decode(u32 count, std::vector<u32> &out) {
    out.clear();
    out.reserve(count);
    for (u32 i = 0; i < count; ++i) {
      u32 v = 0;
      if (!next(v)) {
        if (!failed_) {
          fail("truncated RLE/bit-packed run");
        }
        return false;
      }
      out.push_back(v);
    }
    return true;
  }

 private:
  bool fail(const std::string &msg) {
    if (!failed_) {
      failed_ = true;
      err_ = msg;
    }
    return false;
  }

  bool start_run() {
    u64 header = 0;
    if (pos_ >= size_ || !load_le_varint(data_, size_, pos_, header)) {
      fail("truncated RLE/bit-packed run header");
      return false;
    }
    if ((header & 1) != 0) {
      // Bit-packed run: header = (number of 8-value groups << 1) | 1.
      u64 groups = header >> 1;
      if (groups == 0) {
        fail("empty bit-packed run");
        return false;
      }
      u64 values = groups * 8;
      u64 bytes_needed = groups * static_cast<u64>(bit_width_);
      if (bytes_needed > static_cast<u64>(size_ - pos_)) {
        fail("bit-packed run exceeds level/indices buffer");
        return false;
      }
      if (values > 0x7fffffffULL) {
        fail("bit-packed run is too large");
        return false;
      }
      packed_remaining_ = static_cast<u64>(values);
      bit_buf_ = 0;
      bit_count_ = 0;
      return true;
    }
    // RLE run: header = run length << 1.
    u64 count = header >> 1;
    if (count == 0) {
      fail("empty RLE run");
      return false;
    }
    int value_bytes = (bit_width_ + 7) / 8;
    if (static_cast<size_t>(value_bytes) > size_ - pos_) {
      fail("truncated RLE run value");
      return false;
    }
    u32 v = 0;
    for (int i = 0; i < value_bytes; ++i) {
      v |= static_cast<u32>(data_[pos_++]) << (8 * i);
    }
    if (bit_width_ < 32) {
      v &= (static_cast<u32>(1) << bit_width_) - 1;
    }
    rle_value_ = v;
    rle_remaining_ = count;
    return true;
  }

  bool take_packed(u32 &value) {
    if (bit_width_ == 0) {
      value = 0;
      return true;
    }
    int need = bit_width_;
    while (bit_count_ < need) {
      if (pos_ >= size_) {
        return fail("truncated bit-packed values");
      }
      bit_buf_ |= static_cast<u64>(data_[pos_++]) << bit_count_;
      bit_count_ += 8;
    }
    u32 mask = bit_width_ == 32 ? 0xffffffffU
                                : (static_cast<u32>(1) << bit_width_) - 1;
    value = static_cast<u32>(bit_buf_ & mask);
    bit_buf_ >>= bit_width_;
    bit_count_ -= bit_width_;
    return true;
  }

  const u8 *data_;
  size_t size_;
  size_t pos_;
  int bit_width_;
  u64 rle_remaining_;
  u32 rle_value_;
  u64 packed_remaining_;
  u64 bit_buf_;
  int bit_count_;
  bool failed_;
  std::string err_;
};

bool decode_levels(const u8 *data, size_t size, i64 count, i64 max_level,
                   std::vector<u32> &levels, std::string &err) {
  levels.clear();
  if (count < 0) {
    return set_error(err, "negative level count");
  }
  if (max_level <= 0) {
    levels.assign(static_cast<size_t>(count), 0);
    return true;
  }
  int bit_width = 0;
  while ((static_cast<i64>(1) << bit_width) <= max_level) {
    ++bit_width;
  }
  RleDecoder dec(data, size, bit_width);
  if (!dec.decode(static_cast<u32>(count), levels)) {
    return set_error(err, "invalid definition/repetition levels: " + dec.error());
  }
  for (size_t i = 0; i < levels.size(); ++i) {
    if (static_cast<i64>(levels[i]) > max_level) {
      return set_error(err, "definition level " + int_to_string(levels[i]) +
                                " is out of range (max " + int_to_string(max_level) + ")");
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// PLAIN value decoder
// ---------------------------------------------------------------------------

class PlainValueDecoder {
 public:
  PlainValueDecoder(const u8 *data, size_t size, int physical_type, i64 type_length,
                    bool logical_date)
      : data_(data), size_(size), pos_(0), physical_type_(physical_type),
        type_length_(type_length), logical_date_(logical_date), bit_pos_(0) {}

  bool next(Cell &out, std::string &err) {
    switch (physical_type_) {
      case PT_BOOLEAN: {
        size_t byte_index = bit_pos_ / 8;
        if (byte_index >= size_) {
          return set_error(err, "truncated BOOLEAN value");
        }
        u8 b = data_[byte_index];
        bool v = ((b >> (bit_pos_ % 8)) & 1) != 0;
        ++bit_pos_;
        out = make_bool(v);
        return true;
      }
      case PT_INT32: {
        if (size_ - pos_ < 4) {
          return set_error(err, "truncated INT32 value");
        }
        u32 raw = load_le32(data_ + pos_);
        pos_ += 4;
        i32 v = static_cast<i32>(raw);
        if (logical_date_) {
          std::string s;
          format_days_as_date(v, s);
          out = make_string(s);
        } else {
          out = make_int(static_cast<i64>(v));
        }
        return true;
      }
      case PT_INT64: {
        if (size_ - pos_ < 8) {
          return set_error(err, "truncated INT64 value");
        }
        u64 raw = load_le64(data_ + pos_);
        pos_ += 8;
        out = make_int(static_cast<i64>(raw));
        return true;
      }
      case PT_FLOAT: {
        if (size_ - pos_ < 4) {
          return set_error(err, "truncated FLOAT value");
        }
        u32 raw = load_le32(data_ + pos_);
        pos_ += 4;
        float f = 0.0f;
        std::memcpy(&f, &raw, sizeof(f));
        out = make_double(static_cast<double>(f));
        return true;
      }
      case PT_DOUBLE: {
        if (size_ - pos_ < 8) {
          return set_error(err, "truncated DOUBLE value");
        }
        u64 raw = load_le64(data_ + pos_);
        pos_ += 8;
        double d = 0.0;
        std::memcpy(&d, &raw, sizeof(d));
        out = make_double(d);
        return true;
      }
      case PT_BYTE_ARRAY: {
        if (size_ - pos_ < 4) {
          return set_error(err, "truncated BYTE_ARRAY length");
        }
        u32 len = load_le32(data_ + pos_);
        pos_ += 4;
        if (static_cast<size_t>(len) > size_ - pos_) {
          return set_error(err, "truncated BYTE_ARRAY value");
        }
        out = make_string(std::string(reinterpret_cast<const char *>(data_ + pos_), len));
        pos_ += len;
        return true;
      }
      case PT_FIXED_LEN_BYTE_ARRAY: {
        if (type_length_ < 0 || static_cast<u64>(type_length_) > size_ - pos_) {
          return set_error(err, "truncated FIXED_LEN_BYTE_ARRAY value");
        }
        out = make_string(std::string(reinterpret_cast<const char *>(data_ + pos_),
                                      static_cast<size_t>(type_length_)));
        pos_ += static_cast<size_t>(type_length_);
        return true;
      }
      default:
        return set_error(err, "unsupported parquet physical type " +
                                  int_to_string(physical_type_));
    }
  }

 private:
  const u8 *data_;
  size_t size_;
  size_t pos_;
  int physical_type_;
  i64 type_length_;
  bool logical_date_;
  size_t bit_pos_;
};

// ---------------------------------------------------------------------------
// Column chunk cursor: decodes one column chunk page by page
// ---------------------------------------------------------------------------

class ColumnCursor {
 public:
  ColumnCursor() : reader_path_(nullptr), leaf_index_(0), schema_(nullptr),
                   element_(nullptr), file_meta_(nullptr), chunk_active_(false),
                   values_remaining_(0), cell_pos_(0), dictionary_loaded_(false),
                   saw_data_page_(false), codec_(-1), physical_type_(-1),
                   type_length_(0), max_def_level_(0), logical_date_(false) {}

  void init(const std::string *path, int leaf_index, const ColumnSchema *schema,
            const SchemaElement *element, const FileMetaData *meta) {
    reader_path_ = path;
    leaf_index_ = leaf_index;
    schema_ = schema;
    element_ = element;
    file_meta_ = meta;
  }

  void close() {
    file_.close();
    chunk_active_ = false;
    cells_.clear();
    dictionary_.clear();
  }

  bool begin_row_group(size_t rg_index, std::string &err) {
    if (reader_path_ == nullptr || schema_ == nullptr || element_ == nullptr ||
        file_meta_ == nullptr) {
      return set_error(err, "parquet column cursor is not initialized");
    }
    if (rg_index >= file_meta_->row_groups.size()) {
      return set_error(err, "parquet row group index out of range");
    }
    const RowGroup &rg = file_meta_->row_groups[rg_index];
    if (leaf_index_ < 0 || static_cast<size_t>(leaf_index_) >= rg.columns.size()) {
      return set_error(err, "parquet row group is missing column '" + schema_->name + "'");
    }
    const ColumnChunk &cc = rg.columns[static_cast<size_t>(leaf_index_)];
    if (!cc.has_meta) {
      return set_error(err, "parquet column chunk '" + schema_->name +
                                "' has no metadata");
    }
    if (!cc.file_path.empty()) {
      return set_error(err, "external column chunk files are not supported (column '" +
                                schema_->name + "')");
    }
    const ColumnMetaData &cm = cc.meta;
    if (cm.type != element_->type) {
      return set_error(err, "parquet column chunk type mismatch for column '" +
                                schema_->name + "'");
    }
    if (cm.num_values != rg.num_rows) {
      return set_error(err, "parquet column chunk value count (" +
                                int_to_string(cm.num_values) +
                                ") does not match row group row count (" +
                                int_to_string(rg.num_rows) + ")");
    }
    if (cm.num_values == 0) {
      // Empty row group: there are no pages to read.
      chunk_active_ = true;
      values_remaining_ = 0;
      cells_.clear();
      cell_pos_ = 0;
      dictionary_.clear();
      dictionary_loaded_ = false;
      saw_data_page_ = false;
      codec_ = cm.codec;
      physical_type_ = element_->type;
      type_length_ = element_->type_length;
      logical_date_ = (schema_->type == LogicalType::DATE);
      max_def_level_ = element_->repetition == REP_OPTIONAL ? 1 : 0;
      return true;
    }
    i64 start = cm.data_page_offset;
    if (cm.dictionary_page_offset > 0 &&
        (start <= 0 || cm.dictionary_page_offset < start)) {
      start = cm.dictionary_page_offset;
    }
    if (start <= 0) {
      return set_error(err, "parquet column chunk '" + schema_->name +
                                "' has no data page offset");
    }
    if (!file_.is_open()) {
      if (!file_.open(*reader_path_, err)) {
        return false;
      }
    }
    i64 data_end = file_.size() - 8;
    i64 end = data_end;
    if (cm.total_compressed_size > 0) {
      end = start + cm.total_compressed_size;
      if (end > data_end) {
        // Be lenient with writers that over-report the chunk span: decoding
        // stops as soon as the declared number of values has been produced.
        end = data_end;
      }
    }
    if (start >= data_end || end <= start) {
      return set_error(err, "parquet column chunk '" + schema_->name +
                                "' points outside the data section (truncated file?)");
    }
    if (!file_.seek(start)) {
      return set_error(err, "cannot seek to parquet column chunk '" + schema_->name + "'");
    }
    file_.set_limit(end);
    chunk_active_ = true;
    values_remaining_ = cm.num_values;
    cells_.clear();
    cell_pos_ = 0;
    dictionary_.clear();
    dictionary_loaded_ = false;
    saw_data_page_ = false;
    codec_ = cm.codec;
    physical_type_ = element_->type;
    type_length_ = element_->type_length;
    logical_date_ = (schema_->type == LogicalType::DATE);
    max_def_level_ = element_->repetition == REP_OPTIONAL ? 1 : 0;
    return true;
  }

  bool next_cell(Cell &out, std::string &err) {
    if (!chunk_active_) {
      return set_error(err, "parquet column cursor is not positioned on a row group");
    }
    if (cell_pos_ >= cells_.size()) {
      if (!ensure_cell(err)) {
        return false;
      }
    }
    if (cell_pos_ >= cells_.size()) {
      return set_error(err, "parquet column chunk for '" + schema_->name +
                                "' ended before the row group did");
    }
    out = cells_[cell_pos_++];
    return true;
  }

 private:
  bool ensure_cell(std::string &err) {
    while (cell_pos_ >= cells_.size()) {
      if (values_remaining_ <= 0) {
        return set_error(err, "parquet column chunk for '" + schema_->name +
                                  "' ended before the row group did");
      }
      PageHeaderInfo ph;
      ThriftReader tr(file_);
      if (!parse_page_header(tr, ph, err)) {
        return set_error(err, "malformed page header for column '" + schema_->name +
                                  "': " + err);
      }
      if (ph.compressed_page_size < 0) {
        return set_error(err, "negative parquet page size for column '" + schema_->name + "'");
      }
      if (ph.type == PAGE_INDEX) {
        if (!file_.skip_bytes(ph.compressed_page_size)) {
          return set_error(err, "truncated index page in column '" + schema_->name + "'");
        }
        continue;
      }
      if (ph.type == PAGE_DICTIONARY) {
        if (saw_data_page_) {
          return set_error(err, "dictionary page after data page in column '" +
                                    schema_->name + "'");
        }
        if (dictionary_loaded_) {
          return set_error(err, "duplicate dictionary page in column '" +
                                    schema_->name + "'");
        }
        if (!decode_dictionary_page(ph, err)) {
          return false;
        }
        dictionary_loaded_ = true;
        continue;
      }
      if (ph.type != PAGE_DATA && ph.type != PAGE_DATA_V2) {
        return set_error(err, "unsupported parquet page type " + int_to_string(ph.type) +
                                  " in column '" + schema_->name + "'");
      }
      if (ph.num_values < 0) {
        return set_error(err, "negative page value count in column '" + schema_->name + "'");
      }
      if (ph.num_values > values_remaining_) {
        return set_error(err, "parquet page declares more values than its column chunk "
                              "contains (column '" + schema_->name + "')");
      }
      if (ph.type == PAGE_DATA) {
        if (!decode_data_page_v1(ph, err)) {
          return false;
        }
      } else {
        if (!decode_data_page_v2(ph, err)) {
          return false;
        }
      }
      saw_data_page_ = true;
      values_remaining_ -= ph.num_values;
    }
    return true;
  }

  bool read_page_body(const PageHeaderInfo &ph, std::vector<u8> &body, std::string &err) {
    if (ph.compressed_page_size < 0 ||
        ph.compressed_page_size > file_.remaining()) {
      return set_error(err, "parquet page body is truncated (column '" +
                                schema_->name + "')");
    }
    body.resize(static_cast<size_t>(ph.compressed_page_size));
    if (!body.empty() && !file_.read_bytes(&body[0], body.size())) {
      return set_error(err, "cannot read parquet page body (column '" +
                                schema_->name + "')");
    }
    return true;
  }

  bool decompress(const std::vector<u8> &in, i64 expected, std::vector<u8> &out,
                  std::string &err) {
    if (codec_ == CODEC_UNCOMPRESSED) {
      out = in;
      if (expected >= 0 && static_cast<i64>(out.size()) != expected) {
        return set_error(err, "uncompressed parquet page size mismatch (column '" +
                                  schema_->name + "')");
      }
      return true;
    }
    if (codec_ == CODEC_SNAPPY) {
      std::string serr;
      if (!snappy_decompress(in.empty() ? nullptr : &in[0], in.size(), expected, out, serr)) {
        return set_error(err, serr + " (column '" + schema_->name + "')");
      }
      return true;
    }
    return set_error(err, std::string("unsupported parquet compression codec ") +
                              codec_name(codec_) + " (column '" + schema_->name + "')");
  }

  bool decode_dictionary_page(const PageHeaderInfo &ph, std::string &err) {
    std::vector<u8> body;
    if (!read_page_body(ph, body, err)) {
      return false;
    }
    std::vector<u8> raw;
    if (!decompress(body, ph.uncompressed_page_size, raw, err)) {
      return false;
    }
    if (ph.dictionary_encoding != ENC_PLAIN &&
        ph.dictionary_encoding != ENC_PLAIN_DICTIONARY &&
        ph.dictionary_encoding != -1) {
      return set_error(err, std::string("unsupported dictionary page encoding ") +
                                encoding_name(ph.dictionary_encoding) + " (column '" +
                                schema_->name + "')");
    }
    if (ph.dictionary_num_values < 0) {
      return set_error(err, "negative dictionary page value count (column '" +
                                schema_->name + "')");
    }
    dictionary_.clear();
    dictionary_.reserve(static_cast<size_t>(
        ph.dictionary_num_values < 65536 ? ph.dictionary_num_values : 65536));
    PlainValueDecoder dec(raw.empty() ? nullptr : &raw[0], raw.size(), physical_type_,
                          type_length_, logical_date_);
    for (i64 i = 0; i < ph.dictionary_num_values; ++i) {
      Cell c;
      if (!dec.next(c, err)) {
        return set_error(err, "malformed dictionary page for column '" + schema_->name +
                                  "': " + err);
      }
      dictionary_.push_back(std::move(c));
    }
    return true;
  }

  bool decode_data_page_v1(const PageHeaderInfo &ph, std::string &err) {
    std::vector<u8> body;
    if (!read_page_body(ph, body, err)) {
      return false;
    }
    std::vector<u8> raw;
    if (!decompress(body, ph.uncompressed_page_size, raw, err)) {
      return false;
    }
    size_t offset = 0;
    std::vector<u32> levels;
    if (max_def_level_ > 0) {
      if (ph.definition_level_encoding != ENC_RLE &&
          ph.definition_level_encoding != -1) {
        return set_error(err, std::string("unsupported definition level encoding ") +
                                  encoding_name(ph.definition_level_encoding) +
                                  " (column '" + schema_->name +
                                  "'); only RLE level encoding is supported");
      }
      if (raw.size() - offset < 4) {
        return set_error(err, "truncated definition levels (column '" +
                                  schema_->name + "')");
      }
      u32 level_len = load_le32(&raw[offset]);
      offset += 4;
      if (static_cast<size_t>(level_len) > raw.size() - offset) {
        return set_error(err, "truncated definition levels (column '" +
                                  schema_->name + "')");
      }
      if (!decode_levels(&raw[offset], level_len, ph.num_values, max_def_level_,
                         levels, err)) {
        return set_error(err, "column '" + schema_->name + "': " + err);
      }
      offset += level_len;
    } else {
      levels.assign(static_cast<size_t>(ph.num_values), 0);
    }
    return decode_values(&raw[offset], raw.size() - offset, ph, levels, err);
  }

  bool decode_data_page_v2(const PageHeaderInfo &ph, std::string &err) {
    std::vector<u8> body;
    if (!read_page_body(ph, body, err)) {
      return false;
    }
    i64 rep_len = ph.repetition_levels_byte_length;
    i64 def_len = ph.definition_levels_byte_length;
    if (rep_len < 0 || def_len < 0 ||
        rep_len + def_len > ph.compressed_page_size) {
      return set_error(err, "invalid level lengths in DataPageV2 (column '" +
                                schema_->name + "')");
    }
    size_t levels_len = static_cast<size_t>(rep_len + def_len);
    if (body.size() < levels_len) {
      return set_error(err, "truncated DataPageV2 levels (column '" +
                                schema_->name + "')");
    }
    std::vector<u32> levels;
    if (max_def_level_ > 0) {
      if (def_len > 0) {
        if (!decode_levels(&body[static_cast<size_t>(rep_len)],
                           static_cast<size_t>(def_len), ph.num_values,
                           max_def_level_, levels, err)) {
          return set_error(err, "column '" + schema_->name + "': " + err);
        }
      } else if (ph.num_nulls != 0) {
        return set_error(err, "DataPageV2 is missing definition levels for a "
                              "nullable column ('" + schema_->name + "')");
      } else {
        levels.assign(static_cast<size_t>(ph.num_values), 1);
      }
    } else {
      levels.assign(static_cast<size_t>(ph.num_values), 0);
    }
    i64 expected_values_size = ph.uncompressed_page_size - static_cast<i64>(levels_len);
    if (expected_values_size < 0) {
      return set_error(err, "invalid DataPageV2 sizes (column '" + schema_->name + "')");
    }
    const u8 *values_data = body.empty() ? nullptr : &body[0] + levels_len;
    size_t values_size = body.size() - levels_len;
    std::vector<u8> decompressed;
    if (ph.is_compressed && codec_ != CODEC_UNCOMPRESSED) {
      std::vector<u8> compressed(values_data, values_data + values_size);
      if (!decompress(compressed, expected_values_size, decompressed, err)) {
        return false;
      }
      values_data = decompressed.empty() ? nullptr : &decompressed[0];
      values_size = decompressed.size();
    } else if (static_cast<i64>(values_size) != expected_values_size) {
      return set_error(err, "DataPageV2 size mismatch (column '" + schema_->name + "')");
    }
    return decode_values(values_data, values_size, ph, levels, err);
  }

  bool decode_values(const u8 *data, size_t size, const PageHeaderInfo &ph,
                     const std::vector<u32> &levels, std::string &err) {
    size_t non_null = 0;
    for (size_t i = 0; i < levels.size(); ++i) {
      if (static_cast<i64>(levels[i]) == max_def_level_) {
        ++non_null;
      }
    }
    std::vector<Cell> values;
    values.reserve(non_null);
    if (ph.encoding == ENC_PLAIN) {
      PlainValueDecoder dec(data, size, physical_type_, type_length_, logical_date_);
      for (size_t i = 0; i < non_null; ++i) {
        Cell c;
        if (!dec.next(c, err)) {
          return set_error(err, "malformed PLAIN page for column '" + schema_->name +
                                    "': " + err);
        }
        values.push_back(std::move(c));
      }
    } else if (ph.encoding == ENC_PLAIN_DICTIONARY || ph.encoding == ENC_RLE_DICTIONARY) {
      if (!dictionary_loaded_) {
        return set_error(err, "dictionary encoded page without a dictionary page "
                              "(column '" + schema_->name + "')");
      }
      if (size < 1) {
        return set_error(err, "truncated dictionary indices (column '" +
                                  schema_->name + "')");
      }
      int bit_width = data[0];
      if (bit_width < 0 || bit_width > 32) {
        return set_error(err, "invalid dictionary index bit width (column '" +
                                  schema_->name + "')");
      }
      RleDecoder dec(data + 1, size - 1, bit_width);
      values.reserve(non_null);
      for (size_t i = 0; i < non_null; ++i) {
        u32 index = 0;
        if (!dec.next(index)) {
          return set_error(err, "malformed dictionary indices for column '" +
                                    schema_->name + "': " + dec.error());
        }
        if (static_cast<size_t>(index) >= dictionary_.size()) {
          return set_error(err, "dictionary index " + int_to_string(index) +
                                    " is out of range (column '" + schema_->name + "')");
        }
        values.push_back(dictionary_[index]);
      }
    } else if (ph.encoding == ENC_RLE) {
      // RLE-encoded BOOLEAN values: 4-byte little-endian length prefix followed
      // by the RLE/bit-packed hybrid stream with bit width 1 (Parquet
      // Encodings.md; the length prefix is present in both page versions).
      if (physical_type_ != PT_BOOLEAN) {
        return set_error(err, std::string("RLE value encoding is only supported for "
                                          "BOOLEAN columns, not ") +
                                  physical_type_name(physical_type_) + " (column '" +
                                  schema_->name + "')");
      }
      if (size < 4) {
        return set_error(err, "truncated RLE boolean values (column '" +
                                  schema_->name + "')");
      }
      u32 rle_len = load_le32(data);
      if (static_cast<size_t>(rle_len) > size - 4) {
        return set_error(err, "truncated RLE boolean values (column '" +
                                  schema_->name + "')");
      }
      RleDecoder dec(data + 4, static_cast<size_t>(rle_len), 1);
      values.reserve(non_null);
      for (size_t i = 0; i < non_null; ++i) {
        u32 v = 0;
        if (!dec.next(v)) {
          return set_error(err, "malformed RLE boolean values for column '" +
                                    schema_->name + "': " + dec.error());
        }
        if (v > 1) {
          return set_error(err, "RLE boolean value is out of range (column '" +
                                    schema_->name + "')");
        }
        values.push_back(make_bool(v != 0));
      }
    } else {
      return set_error(err, std::string("unsupported parquet value encoding ") +
                                encoding_name(ph.encoding) + " (column '" +
                                schema_->name + "'); only PLAIN, "
                                "PLAIN_DICTIONARY/RLE_DICTIONARY and RLE(BOOLEAN) "
                                "are supported");
    }
    cells_.clear();
    cells_.reserve(levels.size());
    size_t value_index = 0;
    for (size_t i = 0; i < levels.size(); ++i) {
      if (static_cast<i64>(levels[i]) == max_def_level_) {
        if (value_index >= values.size()) {
          return set_error(err, "not enough values in page (column '" +
                                    schema_->name + "')");
        }
        cells_.push_back(values[value_index++]);
      } else {
        cells_.push_back(make_null());
      }
    }
    if (value_index != values.size()) {
      return set_error(err, "too many values in page (column '" + schema_->name + "')");
    }
    cell_pos_ = 0;
    return true;
  }

  const std::string *reader_path_;
  int leaf_index_;
  const ColumnSchema *schema_;
  const SchemaElement *element_;
  const FileMetaData *file_meta_;
  FileStream file_;
  bool chunk_active_;
  i64 values_remaining_;
  std::vector<Cell> cells_;
  size_t cell_pos_;
  std::vector<Cell> dictionary_;
  bool dictionary_loaded_;
  bool saw_data_page_;
  int codec_;
  int physical_type_;
  i64 type_length_;
  i64 max_def_level_;
  bool logical_date_;
};

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

class ParquetFileReader : public IFileReader {
 public:
  ParquetFileReader()
      : row_group_(0), rows_left_(0), opened_(false), failed_(false),
        group_active_(false) {}
  ~ParquetFileReader() override { close(); }

  void set_projection(const std::vector<std::string> &projection) {
    projection_names_ = projection;
  }

  ReadStatus open(const std::string &path, const FileOptions &opts) override {
    (void)opts;
    close();
    std::string err;
    if (!do_open(path, err)) {
      failed_ = true;
      opened_ = false;
      last_error_ = err;
      return ReadStatus::ERROR;
    }
    failed_ = false;
    opened_ = true;
    return ReadStatus::OK;
  }

  const std::vector<ColumnSchema> &schema() const override { return output_schema_; }

  ReadStatus next_row(std::vector<Cell> &row, std::string &err) override {
    if (!opened_ || failed_) {
      err = last_error_.empty() ? std::string("parquet reader is not open") : last_error_;
      return ReadStatus::ERROR;
    }
    for (;;) {
      if (rows_left_ == 0) {
        if (group_active_) {
          ++row_group_;
          group_active_ = false;
        }
        while (row_group_ < meta_.row_groups.size() &&
               meta_.row_groups[row_group_].num_rows == 0) {
          ++row_group_;
        }
        if (row_group_ >= meta_.row_groups.size()) {
          return ReadStatus::END;
        }
        if (!begin_row_group()) {
          err = last_error_;
          return ReadStatus::ERROR;
        }
        rows_left_ = meta_.row_groups[row_group_].num_rows;
        group_active_ = true;
        continue;
      }
      break;
    }
    row.assign(output_schema_.size(), Cell());
    for (size_t i = 0; i < cursors_.size(); ++i) {
      std::string cerr;
      if (!cursors_[i]->next_cell(row[i], cerr)) {
        last_error_ = cerr;
        failed_ = true;
        err = cerr;
        return ReadStatus::ERROR;
      }
    }
    --rows_left_;
    return ReadStatus::OK;
  }

  void close() override {
    for (size_t i = 0; i < cursors_.size(); ++i) {
      cursors_[i]->close();
    }
    cursors_.clear();
    output_schema_.clear();
    file_schema_.clear();
    schema_elements_.clear();
    meta_ = FileMetaData();
    // projection_names_ is reader configuration and survives open()/close().
    row_group_ = 0;
    rows_left_ = 0;
    group_active_ = false;
    opened_ = false;
    failed_ = false;
    path_.clear();
  }

  const std::string &last_error() const { return last_error_; }

 private:
  bool begin_row_group() {
    last_error_.clear();
    for (size_t i = 0; i < cursors_.size(); ++i) {
      std::string err;
      if (!cursors_[i]->begin_row_group(row_group_, err)) {
        last_error_ = err;
        failed_ = true;
        return false;
      }
    }
    return true;
  }

  bool do_open(const std::string &path, std::string &err) {
    path_ = path;
    FileStream fs;
    if (!fs.open(path, err)) {
      return false;
    }
    if (fs.size() < static_cast<i64>(kParquetMagicLen * 2 + 8)) {
      return set_error(err, "not a parquet file: file is too small");
    }
    u8 magic[4];
    if (!fs.read_bytes(magic, 4)) {
      return set_error(err, "cannot read parquet magic");
    }
    if (std::memcmp(magic, "PAR1", 4) != 0) {
      return set_error(err, "not a parquet file: missing leading PAR1 magic");
    }
    if (!fs.seek(fs.size() - 8)) {
      return set_error(err, "cannot seek to parquet footer");
    }
    u8 tail[8];
    if (!fs.read_bytes(tail, 8)) {
      return set_error(err, "cannot read parquet footer trailer");
    }
    if (std::memcmp(tail + 4, "PAR1", 4) != 0) {
      return set_error(err, "not a parquet file: missing trailing PAR1 magic");
    }
    u32 footer_len = load_le32(tail);
    i64 data_end = fs.size() - 8;
    if (footer_len == 0 || static_cast<i64>(footer_len) > data_end - kParquetMagicLen) {
      return set_error(err, "invalid parquet footer length");
    }
    i64 footer_start = data_end - static_cast<i64>(footer_len);
    fs.set_limit(data_end);
    if (!fs.seek(footer_start)) {
      return set_error(err, "cannot seek to parquet footer metadata");
    }
    {
      ThriftReader tr(fs);
      FileMetaData fm;
      if (!parse_file_metadata(tr, fm, err)) {
        return set_error(err, "invalid parquet footer metadata: " + err);
      }
      meta_ = std::move(fm);
    }
    if (!build_schema(err)) {
      return false;
    }
    if (!validate_row_groups(data_end, err)) {
      return false;
    }
    // Resolve projection.
    std::vector<int> leaves;
    if (projection_names_.empty()) {
      for (size_t i = 0; i < file_schema_.size(); ++i) {
        leaves.push_back(static_cast<int>(i));
      }
    } else {
      for (size_t i = 0; i < projection_names_.size(); ++i) {
        int found = -1;
        for (size_t j = 0; j < file_schema_.size(); ++j) {
          if (file_schema_[j].name == projection_names_[i]) {
            found = static_cast<int>(j);
            break;
          }
        }
        if (found < 0) {
          return set_error(err, "projected column '" + projection_names_[i] +
                                    "' does not exist in the parquet schema");
        }
        leaves.push_back(found);
      }
    }
    output_schema_.clear();
    cursors_.clear();
    for (size_t i = 0; i < leaves.size(); ++i) {
      int leaf = leaves[i];
      std::unique_ptr<ColumnCursor> cursor(new ColumnCursor());
      cursor->init(&path_, leaf, &file_schema_[static_cast<size_t>(leaf)],
                   &schema_elements_[static_cast<size_t>(leaf + 1)], &meta_);
      cursors_.push_back(std::move(cursor));
      output_schema_.push_back(file_schema_[static_cast<size_t>(leaf)]);
    }
    row_group_ = 0;
    rows_left_ = 0;
    group_active_ = false;
    return true;
  }

  bool build_schema(std::string &err) {
    if (meta_.schema.empty()) {
      return set_error(err, "parquet file has no schema");
    }
    if (meta_.num_rows < 0) {
      return set_error(err, "invalid parquet row count");
    }
    const SchemaElement &root = meta_.schema[0];
    if (root.num_children < 0) {
      return set_error(err, "parquet root schema element has no child count");
    }
    if (static_cast<size_t>(root.num_children) + 1 != meta_.schema.size()) {
      return set_error(err, "nested parquet schemas are not supported "
                            "(only a flat root with primitive leaf columns)");
    }
    file_schema_.clear();
    schema_elements_.assign(meta_.schema.begin(), meta_.schema.end());
    for (size_t i = 1; i < meta_.schema.size(); ++i) {
      const SchemaElement &e = meta_.schema[i];
      if (e.num_children > 0) {
        return set_error(err, "nested parquet schemas are not supported (column '" +
                                  e.name + "' has children)");
      }
      if (e.repetition == REP_REPEATED) {
        return set_error(err, "REPEATED parquet columns are not supported (column '" +
                                  e.name + "')");
      }
      if (e.repetition != REP_REQUIRED && e.repetition != REP_OPTIONAL &&
          e.repetition != -1) {
        return set_error(err, "invalid repetition type for parquet column '" + e.name + "'");
      }
      if (e.type == PT_INT96) {
        return set_error(err, "INT96 parquet columns are not supported (column '" +
                                  e.name + "')");
      }
      ColumnSchema col;
      col.name = e.name.empty() ? std::string("column_") + int_to_string(i) : e.name;
      col.nullable = (e.repetition == REP_OPTIONAL);
      bool is_date = (e.converted_type == 6) || (e.annotation == 2);
      switch (e.type) {
        case PT_BOOLEAN:
          col.type = LogicalType::BOOLEAN;
          break;
        case PT_INT32:
          col.type = is_date ? LogicalType::DATE : LogicalType::BIGINT;
          break;
        case PT_INT64:
          col.type = LogicalType::BIGINT;
          break;
        case PT_FLOAT:
        case PT_DOUBLE:
          col.type = LogicalType::DOUBLE;
          break;
        case PT_BYTE_ARRAY:
        case PT_FIXED_LEN_BYTE_ARRAY:
          col.type = LogicalType::VARCHAR;
          break;
        default:
          return set_error(err, std::string("unsupported parquet physical type ") +
                                    physical_type_name(e.type) + " (column '" +
                                    e.name + "')");
      }
      file_schema_.push_back(col);
    }
    if (file_schema_.empty()) {
      return set_error(err, "parquet schema has no columns");
    }
    return true;
  }

  bool validate_row_groups(i64 data_end, std::string &err) {
    if (meta_.num_rows > 0 && meta_.row_groups.empty()) {
      return set_error(err, "parquet file declares rows but has no row groups");
    }
    i64 total_rows = 0;
    for (size_t rg = 0; rg < meta_.row_groups.size(); ++rg) {
      const RowGroup &group = meta_.row_groups[rg];
      if (group.num_rows < 0) {
        return set_error(err, "invalid row count in parquet row group " +
                                  int_to_string(static_cast<i64>(rg)));
      }
      if (group.columns.size() != file_schema_.size()) {
        return set_error(err, "parquet row group " + int_to_string(static_cast<i64>(rg)) +
                                  " has " + int_to_string(static_cast<i64>(group.columns.size())) +
                                  " columns but the schema has " +
                                  int_to_string(static_cast<i64>(file_schema_.size())));
      }
      total_rows += group.num_rows;
      for (size_t c = 0; c < group.columns.size(); ++c) {
        const ColumnChunk &cc = group.columns[c];
        const ColumnSchema &col = file_schema_[c];
        const SchemaElement &elem = schema_elements_[c + 1];
        if (!cc.has_meta) {
          return set_error(err, "parquet column chunk '" + col.name + "' has no metadata");
        }
        if (!cc.file_path.empty()) {
          return set_error(err, "external parquet column chunk files are not supported "
                                "(column '" + col.name + "')");
        }
        const ColumnMetaData &cm = cc.meta;
        if (cm.type != elem.type) {
          return set_error(err, "parquet column chunk type mismatch for column '" +
                                    col.name + "'");
        }
        if (cm.path.size() != 1 || cm.path[0] != elem.name) {
          return set_error(err, "nested parquet column paths are not supported "
                                "(column '" + col.name + "')");
        }
        if (cm.num_values != group.num_rows) {
          return set_error(err, "parquet column chunk value count does not match "
                                "row group row count (column '" + col.name + "')");
        }
        if (cm.num_values == 0) {
          continue;  // empty row group, no pages to validate
        }
        if (cm.codec != CODEC_UNCOMPRESSED && cm.codec != CODEC_SNAPPY) {
          return set_error(err, std::string("unsupported parquet compression codec ") +
                                    codec_name(cm.codec) + " (column '" + col.name +
                                    "'); only UNCOMPRESSED and SNAPPY are supported");
        }
        i64 start = cm.data_page_offset;
        if (cm.dictionary_page_offset > 0 &&
            (start <= 0 || cm.dictionary_page_offset < start)) {
          start = cm.dictionary_page_offset;
        }
        i64 end = cm.total_compressed_size > 0 ? start + cm.total_compressed_size : data_end;
        if (end > data_end) {
          end = data_end;
        }
        if (start <= 0 || start >= data_end || end <= start) {
          return set_error(err, "parquet column chunk '" + col.name +
                                    "' points outside the data section (truncated file?)");
        }
      }
    }
    if (total_rows != meta_.num_rows) {
      return set_error(err, "parquet row group row counts (" + int_to_string(total_rows) +
                                ") do not match the file row count (" +
                                int_to_string(meta_.num_rows) + ")");
    }
    return true;
  }

  std::string path_;
  FileMetaData meta_;
  std::vector<ColumnSchema> file_schema_;
  std::vector<SchemaElement> schema_elements_;
  std::vector<ColumnSchema> output_schema_;
  std::vector<std::string> projection_names_;
  std::vector<std::unique_ptr<ColumnCursor> > cursors_;
  size_t row_group_;
  i64 rows_left_;
  bool opened_;
  bool failed_;
  bool group_active_;
  std::string last_error_;
};

}  // namespace

std::unique_ptr<IFileReader> create_parquet_reader() {
  return std::unique_ptr<IFileReader>(new ParquetFileReader());
}

std::unique_ptr<IFileReader> create_parquet_reader(
    const std::vector<std::string> &projection) {
  ParquetFileReader *reader = new ParquetFileReader();
  reader->set_projection(projection);
  return std::unique_ptr<IFileReader>(reader);
}

std::string parquet_reader_last_error(const IFileReader *reader) {
  const ParquetFileReader *parquet_reader =
      dynamic_cast<const ParquetFileReader *>(reader);
  if (parquet_reader == nullptr) {
    return std::string();
  }
  return parquet_reader->last_error();
}

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
