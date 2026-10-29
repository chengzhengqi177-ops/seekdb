// Standalone (dependency-free) unit test for file_sql_parquet.cpp.
//
// Uses only the C++ standard library plus the reader itself. On top of the
// official fixture (D:\seekdb-work\ref\simple.parquet) it *generates* small
// parquet files byte by byte (Thrift Compact Protocol writer + page builder
// implemented below) in order to cover:
//   - multiple row groups / multiple pages per column chunk
//   - all supported physical types, REQUIRED and OPTIONAL columns
//   - PLAIN and RLE_DICTIONARY encoding (RLE and bit-packed hybrid runs)
//   - DataPageV1 and DataPageV2, UNCOMPRESSED and SNAPPY pages
//   - the Snappy copy operators (via a hand-crafted compressed page)
//   - DATE logical type (INT32 days -> "YYYY-MM-DD")
//   - column projection and projected-out unsupported columns
//   - error paths: bad magic, truncation, GZIP, INT96, REPEATED, nested
//     schemas, corrupt definition levels
//   - a light fuzz pass (truncations + byte flips) that must never crash
//
// Build and run from src/sql/engine/file_scan with run_test_parquet.ps1.

#include "file_sql_parquet.h"
#include "arrow_fixtures.h"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace fs = oceanbase::sql::filesql;

namespace {

int g_checks = 0;
int g_failures = 0;

void check(bool cond, const std::string &what) {
  ++g_checks;
  if (!cond) {
    ++g_failures;
    std::printf("FAIL: %s\n", what.c_str());
  } else {
    std::printf("ok: %s\n", what.c_str());
  }
}

using u8 = std::uint8_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;
using i64 = std::int64_t;

// ---------------------------------------------------------------------------
// Thrift Compact Protocol writer (enough for parquet metadata / page headers).
// ---------------------------------------------------------------------------

class ThriftWriter {
 public:
  std::vector<u8> buf;

  void begin_struct() { last_field_.push_back(0); }
  void end_struct() {
    buf.push_back(0);
    last_field_.pop_back();
  }

  void i32_field(int id, i64 v) {
    field(id, 5);
    zigzag(v);
  }
  void i64_field(int id, i64 v) {
    field(id, 6);
    zigzag(v);
  }
  // Struct-valued field: emits the field header and opens the nested struct.
  void struct_field(int id) {
    field(id, 12);
    begin_struct();
  }
  void bool_field(int id, bool v) { field(id, v ? 1 : 2); }
  void binary_field(int id, const std::string &s) {
    field(id, 8);
    uvarint(s.size());
    buf.insert(buf.end(), s.begin(), s.end());
  }
  void binary_field(int id, const std::vector<u8> &s) {
    field(id, 8);
    uvarint(s.size());
    buf.insert(buf.end(), s.begin(), s.end());
  }
  void struct_list_header(int id, int count) {
    field(id, 9);
    list_header(count, 12);
  }
  void i32_list_header(int id, int count) {
    field(id, 9);
    list_header(count, 5);
  }
  void binary_list_header(int id, int count) {
    field(id, 9);
    list_header(count, 8);
  }
  void i32_list_item(i64 v) { zigzag(v); }
  void binary_list_item(const std::string &s) {
    uvarint(s.size());
    buf.insert(buf.end(), s.begin(), s.end());
  }

 private:
  void field(int id, int type) {
    int last = last_field_.back();
    int delta = id - last;
    if (delta > 0 && delta <= 15) {
      buf.push_back(static_cast<u8>((delta << 4) | type));
    } else {
      buf.push_back(static_cast<u8>(type));
      zigzag(id);
    }
    last_field_.back() = id;
  }
  void list_header(int count, int elem_type) {
    if (count < 15) {
      buf.push_back(static_cast<u8>((count << 4) | elem_type));
    } else {
      buf.push_back(static_cast<u8>(0xf0 | elem_type));
      uvarint(static_cast<u64>(count));
    }
  }
  void uvarint(u64 v) {
    for (;;) {
      u8 b = static_cast<u8>(v & 0x7f);
      v >>= 7;
      if (v != 0) {
        buf.push_back(static_cast<u8>(b | 0x80));
      } else {
        buf.push_back(b);
        return;
      }
    }
  }
  void zigzag(i64 v) {
    u64 n = (static_cast<u64>(v) << 1) ^ static_cast<u64>(v >> 63);
    uvarint(n);
  }

  std::vector<int> last_field_;
};

void put_le32(std::vector<u8> &v, u32 x) {
  for (int i = 0; i < 4; ++i) {
    v.push_back(static_cast<u8>((x >> (8 * i)) & 0xff));
  }
}
void put_le64(std::vector<u8> &v, u64 x) {
  for (int i = 0; i < 8; ++i) {
    v.push_back(static_cast<u8>((x >> (8 * i)) & 0xff));
  }
}
void put_uvarint(std::vector<u8> &v, u64 x) {
  for (;;) {
    u8 b = static_cast<u8>(x & 0x7f);
    x >>= 7;
    if (x != 0) {
      v.push_back(static_cast<u8>(b | 0x80));
    } else {
      v.push_back(b);
      return;
    }
  }
}

// Valid raw Snappy block built from literal runs only.
std::vector<u8> snappy_literal(const std::vector<u8> &in) {
  std::vector<u8> out;
  put_uvarint(out, in.size());
  size_t pos = 0;
  while (pos < in.size()) {
    size_t n = in.size() - pos;
    if (n > 60) {
      n = 60;
    }
    out.push_back(static_cast<u8>((n - 1) << 2));
    out.insert(out.end(), in.begin() + pos, in.begin() + pos + n);
    pos += n;
  }
  return out;
}

// Valid raw Snappy block that expands to `total` zero bytes using the copy
// operator (1 literal byte + copy with 2-byte offset). Used to exercise the
// Snappy copy paths instead of only literals.
std::vector<u8> snappy_zeros_with_copy(size_t total) {
  if (total < 2 || total - 1 > 64) {
    std::vector<u8> fallback(total, 0);
    return snappy_literal(fallback);
  }
  std::vector<u8> out;
  put_uvarint(out, total);
  out.push_back(0x00);  // literal tag, length 1
  out.push_back(0x00);  // the single literal zero byte
  out.push_back(static_cast<u8>(((total - 2) << 2) | 2));  // copy, 2-byte offset
  out.push_back(0x01);
  out.push_back(0x00);
  return out;
}

// Hybrid RLE/bit-packed encoder. Encodes one RLE run when all values are
// equal, otherwise bit-packed groups of 8 (LSB first).
void encode_hybrid(const std::vector<u32> &values, int bit_width,
                   std::vector<u8> &out) {
  if (values.empty()) {
    return;
  }
  bool all_same = true;
  for (size_t i = 1; i < values.size(); ++i) {
    if (values[i] != values[0]) {
      all_same = false;
      break;
    }
  }
  if (all_same) {
    put_uvarint(out, static_cast<u64>(values.size()) << 1);
    int vb = (bit_width + 7) / 8;
    for (int i = 0; i < vb; ++i) {
      out.push_back(static_cast<u8>((values[0] >> (8 * i)) & 0xff));
    }
    return;
  }
  size_t groups = (values.size() + 7) / 8;
  put_uvarint(out, (static_cast<u64>(groups) << 1) | 1);
  for (size_t g = 0; g < groups; ++g) {
    std::vector<u8> group(static_cast<size_t>(bit_width), 0);
    for (size_t i = 0; i < 8; ++i) {
      size_t idx = g * 8 + i;
      u32 v = idx < values.size() ? values[idx] : 0;
      for (int b = 0; b < bit_width; ++b) {
        if (((v >> b) & 1) != 0) {
          size_t bit = i * static_cast<size_t>(bit_width) + static_cast<size_t>(b);
          group[bit / 8] = static_cast<u8>(group[bit / 8] | (1u << (bit % 8)));
        }
      }
    }
    out.insert(out.end(), group.begin(), group.end());
  }
}

std::vector<u8> encode_levels_bitpacked(const std::vector<int> &levels) {
  std::vector<u8> out;
  size_t i = 0;
  while (i < levels.size()) {
    size_t groups = (levels.size() - i + 7) / 8;
    put_uvarint(out, (static_cast<u64>(groups) << 1) | 1);
    for (size_t g = 0; g < groups; ++g) {
      u8 packed = 0;
      for (size_t b = 0; b < 8; ++b) {
        size_t idx = i + g * 8 + b;
        if (idx < levels.size() && levels[idx] != 0) {
          packed = static_cast<u8>(packed | (1u << b));
        }
      }
      out.push_back(packed);
    }
    i += groups * 8;
  }
  return out;
}

std::vector<u8> encode_levels_rle(const std::vector<int> &levels) {
  std::vector<u8> out;
  if (levels.empty()) {
    return out;
  }
  bool all_same = true;
  for (size_t i = 1; i < levels.size(); ++i) {
    if (levels[i] != levels[0]) {
      all_same = false;
      break;
    }
  }
  if (!all_same) {
    return encode_levels_bitpacked(levels);
  }
  put_uvarint(out, static_cast<u64>(levels.size()) << 1);
  out.push_back(levels[0] != 0 ? 1 : 0);
  return out;
}

// Physical type ids used by the generator.
enum GenPhys {
  G_BOOLEAN = 0,
  G_INT32 = 1,
  G_INT64 = 2,
  G_INT96 = 3,
  G_FLOAT = 4,
  G_DOUBLE = 5,
  G_BYTE_ARRAY = 6,
  G_FIXED = 7,
};

// PLAIN encoding of non-null values.
void encode_plain(int phys, i64 type_length, const std::vector<fs::Cell> &values,
                  std::vector<u8> &out) {
  switch (phys) {
    case G_BOOLEAN: {
      for (size_t i = 0; i < values.size(); i += 8) {
        u8 packed = 0;
        for (size_t b = 0; b < 8 && i + b < values.size(); ++b) {
          if (values[i + b].i != 0) {
            packed = static_cast<u8>(packed | (1u << b));
          }
        }
        out.push_back(packed);
      }
      break;
    }
    case G_INT32:
      for (size_t i = 0; i < values.size(); ++i) {
        put_le32(out, static_cast<u32>(static_cast<std::int32_t>(values[i].i)));
      }
      break;
    case G_INT64:
      for (size_t i = 0; i < values.size(); ++i) {
        put_le64(out, static_cast<u64>(values[i].i));
      }
      break;
    case G_FLOAT:
      for (size_t i = 0; i < values.size(); ++i) {
        float f = static_cast<float>(values[i].d);
        u32 bits = 0;
        std::memcpy(&bits, &f, sizeof(bits));
        put_le32(out, bits);
      }
      break;
    case G_DOUBLE:
      for (size_t i = 0; i < values.size(); ++i) {
        u64 bits = 0;
        double d = values[i].d;
        std::memcpy(&bits, &d, sizeof(bits));
        put_le64(out, bits);
      }
      break;
    case G_BYTE_ARRAY:
      for (size_t i = 0; i < values.size(); ++i) {
        put_le32(out, static_cast<u32>(values[i].s.size()));
        out.insert(out.end(), values[i].s.begin(), values[i].s.end());
      }
      break;
    case G_FIXED:
    case G_INT96:
      for (size_t i = 0; i < values.size(); ++i) {
        out.insert(out.end(), values[i].s.begin(), values[i].s.end());
      }
      break;
    default:
      break;
  }
}

bool cell_equal(int phys, const fs::Cell &a, const fs::Cell &b) {
  if (a.is_null || b.is_null) {
    return a.is_null == b.is_null;
  }
  switch (phys) {
    case G_BOOLEAN:
    case G_INT32:
    case G_INT64:
      return a.i == b.i;
    case G_FLOAT:
    case G_DOUBLE:
      return a.d == b.d;
    default:
      return a.s == b.s;
  }
}

struct GenPage {
  std::vector<fs::Cell> values;
  bool snappy = false;                    // compress with the literal encoder
  bool force_plain = false;               // PLAIN page even in a dictionary chunk
  std::vector<u8> custom_stored_values;   // stored bytes (v2 + SNAPPY only)
};

struct GenColumn {
  std::string name;
  int phys = G_INT32;
  int repetition = 0;  // 0 REQUIRED, 1 OPTIONAL
  i64 type_length = 0;
  int converted_type = -1;
  bool dictionary = false;
  bool legacy_dictionary_encoding = false;  // PLAIN_DICTIONARY (2) instead of 8
  bool v2 = false;
  bool rle_levels = false;   // v1 definition levels: RLE run instead of bit-packed
  bool corrupt_levels = false;
  int codec_override = -1;
  std::vector<GenPage> pages;
};

// Builds a parquet file from `row_groups` (schema is taken from the first
// row group; every row group must have the same columns in the same order).
std::vector<u8> build_parquet(
    const std::vector<std::vector<GenColumn> > &row_groups) {
  const u8 magic[4] = {'P', 'A', 'R', '1'};
  std::vector<u8> file(magic, magic + 4);

  struct ChunkInfo {
    std::string name;
    int phys = 0;
    i64 data_page_offset = 0;
    i64 dict_page_offset = 0;
    i64 total_compressed = 0;
    i64 total_uncompressed = 0;
    i64 num_values = 0;
    i64 num_rows = 0;
    int codec = 0;
    std::vector<int> encodings;
  };
  struct RgInfo {
    i64 num_rows = 0;
    std::vector<ChunkInfo> chunks;
  };
  std::vector<RgInfo> rg_infos;

  i64 grand_total_rows = 0;
  for (size_t rgi = 0; rgi < row_groups.size(); ++rgi) {
    RgInfo info;
    i64 rows = -1;
    for (size_t ci = 0; ci < row_groups[rgi].size(); ++ci) {
      const GenColumn &col = row_groups[rgi][ci];
      ChunkInfo chunk;
      chunk.name = col.name;
      chunk.phys = col.phys;
      int codec = col.codec_override >= 0 ? col.codec_override : 0;
      if (col.codec_override < 0) {
        for (size_t p = 0; p < col.pages.size(); ++p) {
          if (col.pages[p].snappy || !col.pages[p].custom_stored_values.empty()) {
            codec = 1;
          }
        }
      }
      chunk.codec = codec;
      chunk.encodings.push_back(0);  // PLAIN
      chunk.encodings.push_back(3);  // RLE (levels)
      if (col.dictionary) {
        chunk.encodings.push_back(8);  // RLE_DICTIONARY
      }

      // Build the chunk-level dictionary first.
      std::vector<fs::Cell> dict;
      if (col.dictionary) {
        for (size_t p = 0; p < col.pages.size(); ++p) {
          for (size_t i = 0; i < col.pages[p].values.size(); ++i) {
            const fs::Cell &c = col.pages[p].values[i];
            if (c.is_null) {
              continue;
            }
            bool found = false;
            for (size_t d = 0; d < dict.size(); ++d) {
              if (cell_equal(col.phys, dict[d], c)) {
                found = true;
                break;
              }
            }
            if (!found) {
              dict.push_back(c);
            }
          }
        }
      }

      const i64 chunk_start = static_cast<i64>(file.size());
      if (col.dictionary) {
        std::vector<u8> raw;
        encode_plain(col.phys, col.type_length, dict, raw);
        std::vector<u8> stored = raw;
        if (codec == 1) {
          stored = snappy_literal(raw);
        }
        chunk.dict_page_offset = static_cast<i64>(file.size());
        ThriftWriter ph;
        ph.begin_struct();
        ph.i32_field(1, 2);  // DICTIONARY_PAGE
        ph.i32_field(2, static_cast<i64>(raw.size()));
        ph.i32_field(3, static_cast<i64>(stored.size()));
        ph.struct_field(7);
        ph.i32_field(1, static_cast<i64>(dict.size()));
        ph.i32_field(2, 0);  // PLAIN
        ph.end_struct();
        ph.end_struct();
        file.insert(file.end(), ph.buf.begin(), ph.buf.end());
        file.insert(file.end(), stored.begin(), stored.end());
      }
      chunk.data_page_offset = static_cast<i64>(file.size());
      i64 total_uncompressed = 0;
      i64 page_values = 0;
      for (size_t pi = 0; pi < col.pages.size(); ++pi) {
        const GenPage &page = col.pages[pi];
        const i64 num_values = static_cast<i64>(page.values.size());
        page_values += num_values;
        std::vector<int> levels;
        std::vector<fs::Cell> non_null;
        if (col.repetition == 1) {
          for (size_t i = 0; i < page.values.size(); ++i) {
            if (page.values[i].is_null) {
              levels.push_back(0);
            } else {
              levels.push_back(1);
              non_null.push_back(page.values[i]);
            }
          }
        } else {
          non_null = page.values;
        }

        std::vector<u8> raw_values;
        int encoding = 0;
        if (col.dictionary && !page.force_plain) {
          encoding = col.legacy_dictionary_encoding ? 2 : 8;
          int bit_width = 0;
          while ((static_cast<size_t>(1) << bit_width) < dict.size()) {
            ++bit_width;
          }
          raw_values.push_back(static_cast<u8>(bit_width));
          std::vector<u32> indices;
          for (size_t i = 0; i < non_null.size(); ++i) {
            u32 idx = 0;
            for (size_t d = 0; d < dict.size(); ++d) {
              if (cell_equal(col.phys, dict[d], non_null[i])) {
                idx = static_cast<u32>(d);
                break;
              }
            }
            indices.push_back(idx);
          }
          encode_hybrid(indices, bit_width, raw_values);
        } else {
          encoding = 0;
          encode_plain(col.phys, col.type_length, non_null, raw_values);
        }

        std::vector<u8> level_bytes;
        if (col.repetition == 1) {
          if (col.corrupt_levels) {
            level_bytes.clear();
            level_bytes.push_back(0x00);  // empty RLE run -> must be rejected
          } else if (col.rle_levels) {
            level_bytes = encode_levels_rle(levels);
          } else {
            level_bytes = encode_levels_bitpacked(levels);
          }
        }

        std::vector<u8> body;
        i64 uncompressed_page_size = 0;
        ThriftWriter ph;
        if (col.v2) {
          std::vector<u8> stored = raw_values;
          bool compressed = false;
          if (codec == 1) {
            if (!page.custom_stored_values.empty()) {
              stored = page.custom_stored_values;
            } else {
              stored = snappy_literal(raw_values);
            }
            compressed = true;
          }
          body.insert(body.end(), level_bytes.begin(), level_bytes.end());
          body.insert(body.end(), stored.begin(), stored.end());
          uncompressed_page_size =
              static_cast<i64>(level_bytes.size() + raw_values.size());
          ph.begin_struct();
          ph.i32_field(1, 3);  // DATA_PAGE_V2
          ph.i32_field(2, uncompressed_page_size);
          ph.i32_field(3, static_cast<i64>(body.size()));
          ph.struct_field(8);
          ph.i32_field(1, num_values);
          i64 nulls = num_values - static_cast<i64>(non_null.size());
          ph.i32_field(2, nulls);
          ph.i32_field(3, num_values);
          ph.i32_field(4, encoding);
          ph.i32_field(5, static_cast<i64>(level_bytes.size()));
          ph.i32_field(6, 0);
          if (codec == 1) {
            ph.bool_field(7, compressed);
          }
          ph.end_struct();
          ph.end_struct();
        } else {
          std::vector<u8> uncompressed_body;
          if (col.repetition == 1) {
            put_le32(uncompressed_body, static_cast<u32>(level_bytes.size()));
            uncompressed_body.insert(uncompressed_body.end(), level_bytes.begin(),
                                     level_bytes.end());
          }
          uncompressed_body.insert(uncompressed_body.end(), raw_values.begin(),
                                   raw_values.end());
          body = uncompressed_body;
          if (codec == 1) {
            body = snappy_literal(uncompressed_body);
          }
          uncompressed_page_size = static_cast<i64>(uncompressed_body.size());
          ph.begin_struct();
          ph.i32_field(1, 0);  // DATA_PAGE
          ph.i32_field(2, uncompressed_page_size);
          ph.i32_field(3, static_cast<i64>(body.size()));
          ph.struct_field(5);
          ph.i32_field(1, num_values);
          ph.i32_field(2, encoding);
          ph.i32_field(3, 3);  // RLE levels
          ph.i32_field(4, 3);
          ph.end_struct();
          ph.end_struct();
        }
        file.insert(file.end(), ph.buf.begin(), ph.buf.end());
        file.insert(file.end(), body.begin(), body.end());
        total_uncompressed += static_cast<i64>(ph.buf.size()) + uncompressed_page_size;
      }
      chunk.num_values = page_values;
      chunk.num_rows = page_values;
      chunk.total_uncompressed = total_uncompressed;
      chunk.total_compressed = static_cast<i64>(file.size()) - chunk_start;
      if (rows < 0) {
        rows = page_values;
      } else if (rows != page_values) {
        std::printf("generator error: inconsistent row counts in a row group\n");
      }
      info.chunks.push_back(chunk);
    }
    if (rows < 0) {
      rows = 0;
    }
    info.num_rows = rows;
    grand_total_rows += rows;
    rg_infos.push_back(info);
  }

  // Footer.
  ThriftWriter meta;
  meta.begin_struct();
  meta.i32_field(1, 2);
  const std::vector<GenColumn> empty_rg;
  const std::vector<GenColumn> &first_rg =
      row_groups.empty() ? empty_rg : row_groups[0];
  meta.struct_list_header(2, static_cast<int>(first_rg.size() + 1));
  meta.begin_struct();  // root
  meta.binary_field(4, std::string("schema"));
  meta.i32_field(5, static_cast<i64>(first_rg.size()));
  meta.end_struct();
  for (size_t c = 0; c < first_rg.size(); ++c) {
    meta.begin_struct();
    meta.i32_field(1, first_rg[c].phys);
    if (first_rg[c].phys == G_FIXED || first_rg[c].phys == G_INT96) {
      meta.i32_field(2, first_rg[c].type_length > 0 ? first_rg[c].type_length
                                                   : (first_rg[c].phys == G_INT96 ? 12 : 0));
    }
    meta.i32_field(3, first_rg[c].repetition);
    meta.binary_field(4, first_rg[c].name);
    if (first_rg[c].converted_type >= 0) {
      meta.i32_field(6, first_rg[c].converted_type);
    }
    meta.end_struct();
  }
  meta.i64_field(3, grand_total_rows);
  meta.struct_list_header(4, static_cast<int>(rg_infos.size()));
  for (size_t rgi = 0; rgi < rg_infos.size(); ++rgi) {
    meta.begin_struct();
    meta.struct_list_header(1, static_cast<int>(rg_infos[rgi].chunks.size()));
    for (size_t c = 0; c < rg_infos[rgi].chunks.size(); ++c) {
      const ChunkInfo &chunk = rg_infos[rgi].chunks[c];
      meta.begin_struct();  // ColumnChunk
      meta.i64_field(2, chunk.data_page_offset);
      meta.struct_field(3);
      meta.i32_field(1, chunk.phys);
      meta.i32_list_header(2, static_cast<int>(chunk.encodings.size()));
      for (size_t e = 0; e < chunk.encodings.size(); ++e) {
        meta.i32_list_item(chunk.encodings[e]);
      }
      meta.binary_list_header(3, 1);
      meta.binary_list_item(chunk.name);
      meta.i32_field(4, chunk.codec);
      meta.i64_field(5, chunk.num_values);
      meta.i64_field(6, chunk.total_uncompressed);
      meta.i64_field(7, chunk.total_compressed);
      meta.i64_field(9, chunk.data_page_offset);
      if (chunk.dict_page_offset > 0) {
        meta.i64_field(11, chunk.dict_page_offset);
      }
      meta.end_struct();
      meta.end_struct();
    }
    meta.i64_field(2, 0);
    meta.i64_field(3, rg_infos[rgi].num_rows);
    meta.end_struct();
  }
  meta.binary_field(6, std::string("standalone test generator"));
  meta.end_struct();

  file.insert(file.end(), meta.buf.begin(), meta.buf.end());
  put_le32(file, static_cast<u32>(meta.buf.size()));
  file.insert(file.end(), magic, magic + 4);
  return file;
}

// Builds a footer-only file (no row groups) from explicit schema elements.
std::vector<u8> build_schema_only_file(bool nested) {
  const u8 magic[4] = {'P', 'A', 'R', '1'};
  std::vector<u8> file(magic, magic + 4);
  ThriftWriter meta;
  meta.begin_struct();
  meta.i32_field(1, 2);
  meta.struct_list_header(2, nested ? 3 : 2);
  meta.begin_struct();  // root
  meta.binary_field(4, std::string("schema"));
  meta.i32_field(5, 1);
  meta.end_struct();
  if (nested) {
    meta.begin_struct();  // a group element with one child
    meta.i32_field(3, 0);
    meta.binary_field(4, std::string("grp"));
    meta.i32_field(5, 1);
    meta.end_struct();
  }
  meta.begin_struct();  // leaf
  meta.i32_field(1, G_INT32);
  meta.i32_field(3, 0);
  meta.binary_field(4, std::string("x"));
  meta.end_struct();
  meta.i64_field(3, 0);
  meta.struct_list_header(4, 0);
  meta.end_struct();
  file.insert(file.end(), meta.buf.begin(), meta.buf.end());
  put_le32(file, static_cast<u32>(meta.buf.size()));
  file.insert(file.end(), magic, magic + 4);
  return file;
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

std::string g_work_dir = "test";

std::string temp_path(const std::string &name) {
  return g_work_dir + "\\" + name;
}

bool write_bytes(const std::string &path, const std::vector<u8> &bytes) {
  std::ofstream out(path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    return false;
  }
  if (!bytes.empty()) {
    out.write(reinterpret_cast<const char *>(&bytes[0]),
              static_cast<std::streamsize>(bytes.size()));
  }
  return out.good();
}

struct ReadResult {
  fs::ReadStatus open_status = fs::ReadStatus::ERROR;
  std::string open_error;
  std::vector<fs::ColumnSchema> schema;
  std::vector<std::vector<fs::Cell> > rows;
  std::string read_error;
  bool read_ok = false;
  bool ended = false;
};

ReadResult read_file(const std::string &path,
                     const std::vector<std::string> &projection,
                     bool use_projection) {
  ReadResult result;
  std::unique_ptr<fs::IFileReader> reader;
  if (use_projection) {
    reader = fs::create_parquet_reader(projection);
  } else {
    reader = fs::create_parquet_reader();
  }
  fs::FileOptions opts;
  result.open_status = reader->open(path, opts);
  if (result.open_status != fs::ReadStatus::OK) {
    result.open_error = fs::parquet_reader_last_error(reader.get());
    return result;
  }
  result.schema = reader->schema();
  std::vector<fs::Cell> row;
  std::string err;
  for (int guard = 0; guard < 1000000; ++guard) {
    fs::ReadStatus st = reader->next_row(row, err);
    if (st == fs::ReadStatus::OK) {
      result.rows.push_back(row);
      continue;
    }
    if (st == fs::ReadStatus::END) {
      result.ended = true;
      result.read_ok = true;
    } else {
      result.read_error = err;
    }
    break;
  }
  return result;
}

bool contains(const std::string &haystack, const char *needle) {
  return haystack.find(needle) != std::string::npos;
}

// Minimal base64 decoder for the embedded genuine-Arrow fixtures.
std::vector<u8> base64_decode(const char *text) {
  std::vector<u8> out;
  unsigned value = 0;
  int bits = 0;
  for (const char *p = text; *p != '\0'; ++p) {
    int d = -1;
    char c = *p;
    if (c >= 'A' && c <= 'Z') {
      d = c - 'A';
    } else if (c >= 'a' && c <= 'z') {
      d = c - 'a' + 26;
    } else if (c >= '0' && c <= '9') {
      d = c - '0' + 52;
    } else if (c == '+') {
      d = 62;
    } else if (c == '/') {
      d = 63;
    } else {
      continue;  // '=' padding or whitespace
    }
    value = (value << 6) | static_cast<unsigned>(d);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push_back(static_cast<u8>((value >> bits) & 0xff));
      if (bits > 0) {
        value &= (1u << bits) - 1u;
      } else {
        value = 0;
      }
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void test_simple_parquet(const std::string &path) {
  std::printf("\n== simple.parquet (%s) ==\n", path.c_str());
  ReadResult result = read_file(path, std::vector<std::string>(), false);
  check(result.open_status == fs::ReadStatus::OK,
        "simple.parquet opens (error: '" + result.open_error + "')");
  if (result.open_status != fs::ReadStatus::OK) {
    return;
  }
  check(result.schema.size() == 1, "simple.parquet has exactly one column");
  if (result.schema.size() == 1) {
    check(result.schema[0].name == "c1", "column name is c1");
    check(result.schema[0].type == fs::LogicalType::BIGINT, "column type is BIGINT");
    check(result.schema[0].nullable, "column c1 is nullable (OPTIONAL in the file)");
  }
  check(result.read_ok && result.ended, "simple.parquet is read to the end");
  check(result.rows.size() == 7, "simple.parquet has 7 rows");
  i64 sum = 0;
  i64 mn = 0;
  i64 mx = 0;
  bool values_ok = result.rows.size() == 7;
  for (size_t i = 0; i < result.rows.size(); ++i) {
    if (result.rows[i].size() != 1 || result.rows[i][0].is_null) {
      values_ok = false;
      continue;
    }
    i64 v = result.rows[i][0].i;
    if (v != static_cast<i64>(i)) {
      values_ok = false;
    }
    if (i == 0) {
      mn = mx = v;
    }
    if (v < mn) {
      mn = v;
    }
    if (v > mx) {
      mx = v;
    }
    sum += v;
  }
  check(values_ok, "simple.parquet rows are 0,1,2,3,4,5,6");
  check(sum == 21, "sum(c1) == 21");
  check(mn == 0 && mx == 6, "min(c1) == 0 and max(c1) == 6");
  check(result.rows.size() == 7, "count(*) == 7");
}

std::vector<std::vector<GenColumn> > make_types_file() {
  // Row group 1: 3 rows, row group 2: 4 rows; 8 columns.
  std::vector<GenColumn> rg1(8);
  std::vector<GenColumn> rg2(8);
  for (int i = 0; i < 8; ++i) {
    rg1[i].name = rg2[i].name = std::string("c") + static_cast<char>('0' + i);
  }
  // c0: INT64 REQUIRED, plain v1, two pages in the first row group.
  rg1[0].phys = G_INT64;
  rg1[0].pages.resize(2);
  rg1[0].pages[0].values = {fs::make_int(1), fs::make_int(-2)};
  rg1[0].pages[1].values = {fs::make_int(300000000000LL)};
  rg2[0].phys = G_INT64;
  rg2[0].pages.resize(1);
  rg2[0].pages[0].values = {fs::make_int(4), fs::make_int(5), fs::make_int(6),
                            fs::make_int(-7)};

  // c1: BYTE_ARRAY OPTIONAL, dictionary encoded. Row group 1 is a SNAPPY
  // compressed DataPageV1; row group 2 is DataPageV2, uses the legacy
  // PLAIN_DICTIONARY encoding id and falls back to PLAIN for one page.
  rg1[1].phys = G_BYTE_ARRAY;
  rg1[1].repetition = 1;
  rg1[1].dictionary = true;
  rg1[1].pages.resize(1);
  rg1[1].pages[0].values = {fs::make_string("alpha"), fs::make_null(),
                            fs::make_string("beta")};
  rg1[1].pages[0].snappy = true;
  rg2[1].phys = G_BYTE_ARRAY;
  rg2[1].repetition = 1;
  rg2[1].dictionary = true;
  rg2[1].legacy_dictionary_encoding = true;
  rg2[1].v2 = true;
  rg2[1].pages.resize(2);
  rg2[1].pages[0].values = {fs::make_string("beta"), fs::make_string("gamma")};
  rg2[1].pages[0].snappy = true;
  rg2[1].pages[1].values = {fs::make_null(), fs::make_string("alpha")};
  rg2[1].pages[1].snappy = true;
  rg2[1].pages[1].force_plain = true;

  // c2: DOUBLE REQUIRED, plain v2 (uncompressed).
  rg1[2].phys = G_DOUBLE;
  rg1[2].v2 = true;
  rg1[2].pages.resize(1);
  rg1[2].pages[0].values = {fs::make_double(1.5), fs::make_double(-2.25),
                            fs::make_double(1e300)};
  rg2[2].phys = G_DOUBLE;
  rg2[2].v2 = true;
  rg2[2].pages.resize(1);
  rg2[2].pages[0].values = {fs::make_double(0.0), fs::make_double(-0.5),
                            fs::make_double(3.25), fs::make_double(42.0)};

  // c3: BOOLEAN OPTIONAL, plain v1. Row group 1 uses an RLE definition-level
  // run (all values defined); row group 2 is SNAPPY compressed and has a NULL.
  rg1[3].phys = G_BOOLEAN;
  rg1[3].repetition = 1;
  rg1[3].rle_levels = true;
  rg1[3].pages.resize(1);
  rg1[3].pages[0].values = {fs::make_bool(true), fs::make_bool(false),
                            fs::make_bool(true)};
  rg2[3].phys = G_BOOLEAN;
  rg2[3].repetition = 1;
  rg2[3].pages.resize(1);
  rg2[3].pages[0].values = {fs::make_bool(false), fs::make_null(),
                            fs::make_bool(true), fs::make_bool(false)};
  rg2[3].pages[0].snappy = true;

  // c4: FIXED_LEN_BYTE_ARRAY(3) REQUIRED, plain v1.
  rg1[4].phys = G_FIXED;
  rg1[4].type_length = 3;
  rg1[4].pages.resize(1);
  rg1[4].pages[0].values = {fs::make_string("abc"), fs::make_string("def"),
                            fs::make_string("ghi")};
  rg2[4].phys = G_FIXED;
  rg2[4].type_length = 3;
  rg2[4].pages.resize(1);
  rg2[4].pages[0].values = {fs::make_string("jkl"), fs::make_string("mno"),
                            fs::make_string("pqr"), fs::make_string("stu")};

  // c5: INT32 DATE OPTIONAL (days since epoch -> YYYY-MM-DD), plain v1.
  rg1[5].phys = G_INT32;
  rg1[5].repetition = 1;
  rg1[5].converted_type = 6;  // DATE
  rg1[5].pages.resize(1);
  rg1[5].pages[0].values = {fs::make_int(0), fs::make_null(), fs::make_int(19723)};
  rg2[5].phys = G_INT32;
  rg2[5].repetition = 1;
  rg2[5].converted_type = 6;
  rg2[5].pages.resize(1);
  rg2[5].pages[0].values = {fs::make_int(1), fs::make_int(20000), fs::make_null(),
                            fs::make_int(365)};

  // c6: FLOAT REQUIRED, plain v1, SNAPPY compressed (literal blocks only).
  rg1[6].phys = G_FLOAT;
  rg1[6].pages.resize(1);
  rg1[6].pages[0].values = {fs::make_double(0.5f), fs::make_double(-1.25f),
                            fs::make_double(3.0f)};
  rg1[6].pages[0].snappy = true;
  rg2[6].phys = G_FLOAT;
  rg2[6].pages.resize(1);
  rg2[6].pages[0].values = {fs::make_double(7.0f), fs::make_double(-8.5f),
                            fs::make_double(9.25f), fs::make_double(10.0f)};
  rg2[6].pages[0].snappy = true;

  // c7: INT64 REQUIRED, plain v2 + SNAPPY; also one page stored with the
  // hand-crafted copy-operator Snappy block.
  rg1[7].phys = G_INT64;
  rg1[7].v2 = true;
  rg1[7].pages.resize(1);
  rg1[7].pages[0].values = {fs::make_int(0), fs::make_int(0), fs::make_int(0)};
  rg1[7].pages[0].custom_stored_values = snappy_zeros_with_copy(24);
  rg2[7].phys = G_INT64;
  rg2[7].v2 = true;
  rg2[7].pages.resize(1);
  rg2[7].pages[0].values = {fs::make_int(11), fs::make_int(12), fs::make_int(13),
                            fs::make_int(14)};
  rg2[7].pages[0].snappy = true;

  // Third row group is empty: the reader must skip it.
  std::vector<GenColumn> rg3(8);
  for (int i = 0; i < 8; ++i) {
    rg3[i].name = rg1[i].name;
    rg3[i].phys = rg1[i].phys;
    rg3[i].repetition = rg1[i].repetition;
    rg3[i].type_length = rg1[i].type_length;
    rg3[i].converted_type = rg1[i].converted_type;
    rg3[i].pages.clear();
  }

  std::vector<std::vector<GenColumn> > row_groups;
  row_groups.push_back(rg1);
  row_groups.push_back(rg2);
  row_groups.push_back(rg3);
  return row_groups;
}

void test_generated_types() {
  std::printf("\n== generated multi-row-group / all-types file ==\n");
  std::vector<u8> bytes = build_parquet(make_types_file());
  std::string path = temp_path("generated_types.parquet");
  check(write_bytes(path, bytes), "write generated_types.parquet");
  ReadResult result = read_file(path, std::vector<std::string>(), false);
  check(result.open_status == fs::ReadStatus::OK,
        "generated file opens (error: '" + result.open_error + "')");
  if (result.open_status != fs::ReadStatus::OK) {
    return;
  }
  check(result.schema.size() == 8, "generated file has 8 columns");
  if (result.schema.size() == 8) {
    check(result.schema[0].name == "c0" &&
              result.schema[0].type == fs::LogicalType::BIGINT &&
              !result.schema[0].nullable,
          "c0 INT64 REQUIRED -> BIGINT not null");
    check(result.schema[1].name == "c1" &&
              result.schema[1].type == fs::LogicalType::VARCHAR &&
              result.schema[1].nullable,
          "c1 BYTE_ARRAY OPTIONAL -> VARCHAR nullable");
    check(result.schema[2].type == fs::LogicalType::DOUBLE, "c2 DOUBLE -> DOUBLE");
    check(result.schema[3].type == fs::LogicalType::BOOLEAN, "c3 BOOLEAN -> BOOLEAN");
    check(result.schema[4].type == fs::LogicalType::VARCHAR,
          "c4 FIXED_LEN_BYTE_ARRAY -> VARCHAR");
    check(result.schema[5].type == fs::LogicalType::DATE,
          "c5 INT32(DATE) -> DATE");
    check(result.schema[6].type == fs::LogicalType::DOUBLE, "c6 FLOAT -> DOUBLE");
    check(result.schema[7].type == fs::LogicalType::BIGINT, "c7 INT64 -> BIGINT");
  }
  check(result.read_ok && result.ended, "generated file is read to the end");
  check(result.rows.size() == 7, "generated file has 7 rows (3 + 4)");
  if (result.rows.size() != 7) {
    return;
  }
  // c0: INT64, multiple pages in row group 1.
  const i64 c0[7] = {1, -2, 300000000000LL, 4, 5, 6, -7};
  bool c0_ok = true;
  for (int i = 0; i < 7; ++i) {
    if (result.rows[i][0].is_null || result.rows[i][0].i != c0[i]) {
      c0_ok = false;
    }
  }
  check(c0_ok, "c0 INT64 values across pages/row groups");
  // c1: dictionary encoded VARCHAR with nulls.
  const char *c1[7] = {"alpha", nullptr, "beta", "beta", "gamma", nullptr, "alpha"};
  bool c1_ok = true;
  for (int i = 0; i < 7; ++i) {
    if (c1[i] == nullptr) {
      if (!result.rows[i][1].is_null) {
        c1_ok = false;
      }
    } else if (result.rows[i][1].is_null || result.rows[i][1].s != c1[i]) {
      c1_ok = false;
    }
  }
  check(c1_ok, "c1 dictionary VARCHAR values and NULLs");
  // c2: DOUBLE from DataPageV2.
  const double c2[7] = {1.5, -2.25, 1e300, 0.0, -0.5, 3.25, 42.0};
  bool c2_ok = true;
  for (int i = 0; i < 7; ++i) {
    if (result.rows[i][2].is_null || result.rows[i][2].d != c2[i]) {
      c2_ok = false;
    }
  }
  check(c2_ok, "c2 DOUBLE values from DataPageV2");
  // c3: BOOLEAN with a NULL.
  check(result.rows[0][3].i == 1 && result.rows[1][3].i == 0 &&
            result.rows[2][3].i == 1,
        "c3 BOOLEAN values (row group 1)");
  check(result.rows[4][3].is_null, "c3 BOOLEAN NULL (row group 2)");
  // c4: FIXED_LEN_BYTE_ARRAY.
  check(result.rows[0][4].s == "abc" && result.rows[6][4].s == "stu",
        "c4 FIXED_LEN_BYTE_ARRAY values");
  // c5: DATE formatting.
  check(result.rows[0][5].s == "1970-01-01" && !result.rows[0][5].is_null,
        "c5 DATE day 0 -> 1970-01-01");
  check(result.rows[1][5].is_null, "c5 DATE NULL");
  check(result.rows[2][5].s == "2024-01-01", "c5 DATE day 19723 -> 2024-01-01");
  check(result.rows[3][5].s == "1970-01-02", "c5 DATE day 1 -> 1970-01-02");
  // c6: FLOAT from snappy compressed v1 pages.
  check(result.rows[0][6].d == 0.5 && result.rows[6][6].d == 10.0,
        "c6 FLOAT values from snappy v1 pages");
  // c7: v2 snappy, including the copy-operator snappy page.
  check(result.rows[0][7].i == 0 && result.rows[2][7].i == 0,
        "c7 INT64 zeros from snappy copy-operator page");
  check(result.rows[3][7].i == 11 && result.rows[6][7].i == 14,
        "c7 INT64 from v2 snappy page");
  std::remove(path.c_str());
}

// Checks the common table of the embedded genuine Arrow/parquet-cpp fixtures.
void check_arrow_fixture_table(const ReadResult &result, const std::string &tag) {
  check(result.open_status == fs::ReadStatus::OK,
        tag + " opens (error: '" + result.open_error + "')");
  if (result.open_status != fs::ReadStatus::OK) {
    return;
  }
  check(result.schema.size() == 7, tag + " has 7 columns");
  if (result.schema.size() != 7) {
    return;
  }
  check(result.schema[0].name == "id" &&
            result.schema[0].type == fs::LogicalType::BIGINT &&
            !result.schema[0].nullable,
        tag + ": id INT64 REQUIRED -> BIGINT not null");
  check(result.schema[1].name == "name" &&
            result.schema[1].type == fs::LogicalType::VARCHAR &&
            result.schema[1].nullable,
        tag + ": name STRING -> VARCHAR nullable");
  check(result.schema[2].type == fs::LogicalType::DOUBLE, tag + ": score DOUBLE");
  check(result.schema[3].type == fs::LogicalType::BOOLEAN, tag + ": active BOOLEAN");
  check(result.schema[4].type == fs::LogicalType::VARCHAR,
        tag + ": fixed FIXED_LEN_BYTE_ARRAY -> VARCHAR");
  check(result.schema[5].type == fs::LogicalType::DATE, tag + ": day DATE");
  check(result.schema[6].type == fs::LogicalType::DOUBLE, tag + ": ratio FLOAT -> DOUBLE");
  check(result.read_ok && result.ended, tag + " reads to the end");
  check(result.rows.size() == 7, tag + " has 7 rows");
  if (result.rows.size() != 7) {
    check(false, tag + " read error: " + result.read_error);
    return;
  }
  const i64 ids[7] = {1, -2, 300000000000LL, 4, 5, 6, -7};
  const char *names[7] = {"alpha", nullptr, "beta", "beta", "gamma", nullptr, "alpha"};
  const double scores[7] = {1.5, -2.25, 1e300, 0.0, -0.5, 3.25, 42.0};
  const int active[7] = {1, 0, 1, 0, -1, 1, 0};  // -1 means NULL
  const char *fixed[7] = {"abc", "def", "ghi", "jkl", "mno", "pqr", "stu"};
  const char *days[7] = {"1970-01-01", nullptr, "2024-01-01", "1970-01-02",
                         "2024-10-04", nullptr, "1971-01-01"};
  const double ratios[7] = {0.5, -1.25, 3.0, 7.0, -8.5, 9.25, 10.0};
  bool all_ok = true;
  for (int i = 0; i < 7; ++i) {
    const std::vector<fs::Cell> &row = result.rows[i];
    if (row.size() != 7) {
      all_ok = false;
      continue;
    }
    if (row[0].is_null || row[0].i != ids[i]) all_ok = false;
    if (names[i] == nullptr) {
      if (!row[1].is_null) all_ok = false;
    } else if (row[1].is_null || row[1].s != names[i]) {
      all_ok = false;
    }
    if (row[2].is_null || row[2].d != scores[i]) all_ok = false;
    if (active[i] < 0) {
      if (!row[3].is_null) all_ok = false;
    } else if (row[3].is_null || row[3].i != active[i]) {
      all_ok = false;
    }
    if (row[4].is_null || row[4].s != fixed[i]) all_ok = false;
    if (days[i] == nullptr) {
      if (!row[5].is_null) all_ok = false;
    } else if (row[5].is_null || row[5].s != days[i]) {
      all_ok = false;
    }
    if (row[6].is_null || row[6].d != ratios[i]) all_ok = false;
  }
  check(all_ok, tag + ": all 7 rows and 7 columns match expected values");
}

void test_real_arrow_fixtures() {
  std::printf("\n== genuine Arrow / parquet-cpp files (embedded) ==\n");
  struct Fixture {
    const char *tag;
    const char *b64;
    const char *file_name;
  };
  const Fixture fixtures[] = {
      {"Arrow v1 page format", kArrowV1ParquetB64, "arrow_v1.parquet"},
      {"Arrow v2 page format", kArrowV2ParquetB64, "arrow_v2.parquet"},
  };
  for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); ++i) {
    std::vector<u8> bytes = base64_decode(fixtures[i].b64);
    std::string path = temp_path(fixtures[i].file_name);
    check(!bytes.empty() && write_bytes(path, bytes), std::string("write ") +
                                                          fixtures[i].file_name);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check_arrow_fixture_table(result, fixtures[i].tag);
    std::remove(path.c_str());
  }
  // Genuine nested and INT96 files must be rejected with clear errors.
  {
    std::vector<u8> bytes = base64_decode(kArrowNestedParquetB64);
    std::string path = temp_path("arrow_nested.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR,
          "genuine nested Arrow file is rejected");
    check(contains(result.open_error, "nested"),
          "genuine nested Arrow file error mentions nested");
    std::remove(path.c_str());
  }
  {
    std::vector<u8> bytes = base64_decode(kArrowInt96ParquetB64);
    std::string path = temp_path("arrow_int96.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR,
          "genuine INT96 Arrow file is rejected");
    check(contains(result.open_error, "INT96"),
          "genuine INT96 Arrow file error mentions INT96");
    std::remove(path.c_str());
  }
}

void test_projection() {
  std::printf("\n== column projection ==\n");
  std::vector<u8> bytes = build_parquet(make_types_file());
  std::string path = temp_path("generated_projection.parquet");
  check(write_bytes(path, bytes), "write generated_projection.parquet");
  std::vector<std::string> projection;
  projection.push_back("c1");
  projection.push_back("c0");
  ReadResult result = read_file(path, projection, true);
  check(result.open_status == fs::ReadStatus::OK,
        "projected open succeeds (error: '" + result.open_error + "')");
  check(result.schema.size() == 2, "projection yields 2 columns");
  if (result.schema.size() == 2) {
    check(result.schema[0].name == "c1" && result.schema[1].name == "c0",
          "projection order follows the requested order");
  }
  if (result.rows.size() == 7) {
    check(result.rows[0][1].i == 1 && result.rows[2][0].s == "beta",
          "projected row values are correct");
  } else {
    check(false, "projected read returns 7 rows");
  }
  std::vector<std::string> missing;
  missing.push_back("does_not_exist");
  ReadResult bad = read_file(path, missing, true);
  check(bad.open_status == fs::ReadStatus::ERROR,
        "unknown projected column is rejected");
  check(contains(bad.open_error, "does_not_exist"),
        "unknown projected column error names the column");
  std::remove(path.c_str());
}

void test_errors() {
  std::printf("\n== error handling ==\n");
  // Wrong magic.
  {
    std::vector<u8> bytes = build_parquet(make_types_file());
    bytes[0] = 'X';
    std::string path = temp_path("bad_magic.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "bad leading magic is rejected");
    check(contains(result.open_error, "PAR1"), "bad magic error mentions PAR1");
    std::remove(path.c_str());
  }
  // Trailing magic.
  {
    std::vector<u8> bytes = build_parquet(make_types_file());
    bytes[bytes.size() - 1] = 'X';
    std::string path = temp_path("bad_trailer.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "bad trailing magic is rejected");
    std::remove(path.c_str());
  }
  // Tiny file.
  {
    std::vector<u8> bytes(3, 0);
    std::string path = temp_path("tiny.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "tiny file is rejected");
    std::remove(path.c_str());
  }
  // Unsupported compression (GZIP).
  {
    std::vector<std::vector<GenColumn> > rgs(1);
    GenColumn col;
    col.name = "g";
    col.phys = G_INT32;
    col.codec_override = 2;  // GZIP
    col.pages.resize(1);
    col.pages[0].values = {fs::make_int(1)};
    rgs[0].push_back(col);
    std::vector<u8> bytes = build_parquet(rgs);
    std::string path = temp_path("gzip.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "GZIP compressed file is rejected");
    check(contains(result.open_error, "GZIP") ||
              contains(result.open_error, "unsupported parquet compression"),
          "GZIP error explains the unsupported codec");
    std::remove(path.c_str());
  }
  // INT96.
  {
    std::vector<std::vector<GenColumn> > rgs(1);
    GenColumn col;
    col.name = "ts";
    col.phys = G_INT96;
    col.pages.resize(1);
    col.pages[0].values = {fs::make_string(std::string(12, '\0'))};
    rgs[0].push_back(col);
    std::vector<u8> bytes = build_parquet(rgs);
    std::string path = temp_path("int96.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "INT96 column is rejected");
    check(contains(result.open_error, "INT96"), "INT96 error mentions INT96");
    std::remove(path.c_str());
  }
  // REPEATED.
  {
    std::vector<std::vector<GenColumn> > rgs(1);
    GenColumn col;
    col.name = "r";
    col.phys = G_INT32;
    col.repetition = 2;  // REPEATED
    col.pages.resize(1);
    col.pages[0].values = {fs::make_int(1)};
    rgs[0].push_back(col);
    std::vector<u8> bytes = build_parquet(rgs);
    std::string path = temp_path("repeated.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "REPEATED column is rejected");
    check(contains(result.open_error, "REPEATED"), "REPEATED error mentions REPEATED");
    std::remove(path.c_str());
  }
  // Nested schema.
  {
    std::vector<u8> bytes = build_schema_only_file(true);
    std::string path = temp_path("nested.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "nested schema is rejected");
    check(contains(result.open_error, "nested"), "nested schema error mentions nested");
    std::remove(path.c_str());
  }
  // Truncated file (footer magic survives but its footer length does not).
  {
    std::vector<u8> bytes = build_parquet(make_types_file());
    bytes.resize(bytes.size() - 12);
    std::string path = temp_path("truncated.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR,
          "truncated file is rejected (error: '" + result.open_error + "')");
    std::remove(path.c_str());
  }
  // Corrupt definition levels (read-time error, open succeeds).
  {
    std::vector<std::vector<GenColumn> > rgs(1);
    GenColumn col;
    col.name = "lv";
    col.phys = G_INT32;
    col.repetition = 1;
    col.corrupt_levels = true;
    col.pages.resize(1);
    col.pages[0].values = {fs::make_int(1), fs::make_null()};
    rgs[0].push_back(col);
    std::vector<u8> bytes = build_parquet(rgs);
    std::string path = temp_path("bad_levels.parquet");
    write_bytes(path, bytes);
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::OK, "bad-levels file opens");
    check(!result.read_ok && !result.read_error.empty(),
          "corrupt definition levels produce a read error");
    check(contains(result.read_error, "definition") ||
              contains(result.read_error, "RLE"),
          "level error message is descriptive");
    std::remove(path.c_str());
  }
  // Not a parquet file at all (plain text).
  {
    std::string path = temp_path("not_parquet.txt");
    std::string text = "id,name\n1,alice\n";
    std::ofstream out(path.c_str(), std::ios::binary);
    out.write(text.data(), static_cast<std::streamsize>(text.size()));
    out.close();
    ReadResult result = read_file(path, std::vector<std::string>(), false);
    check(result.open_status == fs::ReadStatus::ERROR, "text file is rejected");
    std::remove(path.c_str());
  }
  // Missing file.
  {
    ReadResult result = read_file(temp_path("missing.parquet"), std::vector<std::string>(),
                                  false);
    check(result.open_status == fs::ReadStatus::ERROR, "missing file is rejected");
    check(contains(result.open_error, "cannot open"),
          "missing file error mentions open failure");
  }
}

void fuzz_one(const std::vector<u8> &bytes, const std::string &label) {
  std::string path = temp_path("fuzz_tmp.parquet");
  if (!write_bytes(path, bytes)) {
    check(false, "fuzz: write " + label);
    return;
  }
  std::unique_ptr<fs::IFileReader> reader = fs::create_parquet_reader();
  fs::FileOptions opts;
  fs::ReadStatus st = reader->open(path, opts);
  if (st == fs::ReadStatus::OK) {
    std::vector<fs::Cell> row;
    std::string err;
    for (int guard = 0; guard < 100000; ++guard) {
      fs::ReadStatus rs = reader->next_row(row, err);
      if (rs != fs::ReadStatus::OK) {
        break;
      }
    }
  }
  std::remove(path.c_str());
}

void test_fuzz() {
  std::printf("\n== fuzz-ish robustness (truncation / byte flips) ==\n");
  std::vector<u8> bytes = build_parquet(make_types_file());
  int cases = 0;
  for (size_t len = 0; len <= bytes.size(); len += 7) {
    std::vector<u8> truncated(bytes.begin(), bytes.begin() + len);
    fuzz_one(truncated, "truncate");
    ++cases;
  }
  for (size_t pos = 0; pos < bytes.size(); pos += 11) {
    std::vector<u8> mutated = bytes;
    mutated[pos] = static_cast<u8>(mutated[pos] ^ 0x5a);
    fuzz_one(mutated, "flip");
    ++cases;
  }
  check(cases > 0, "fuzz pass executed without crashing (" +
                       std::to_string(cases) + " inputs)");
  check(true, "reader survived truncation and byte-flip fuzzing");
}

void test_empty_row_group() {
  std::printf("\n== schema-only file / zero rows ==\n");
  std::vector<u8> bytes = build_schema_only_file(false);
  std::string path = temp_path("empty.parquet");
  write_bytes(path, bytes);
  ReadResult result = read_file(path, std::vector<std::string>(), false);
  check(result.open_status == fs::ReadStatus::OK,
        "zero-row parquet opens (error: '" + result.open_error + "')");
  check(result.schema.size() == 1, "zero-row parquet reports its schema");
  check(result.ended, "zero-row parquet ends immediately");
  check(result.rows.empty(), "zero-row parquet yields no rows");
  std::remove(path.c_str());
}

std::string find_simple_parquet(int argc, char **argv) {
  if (argc > 1) {
    std::ifstream in(argv[1], std::ios::binary);
    if (in.is_open()) {
      return std::string(argv[1]);
    }
  }
  const char *candidates[] = {
      "D:\\seekdb-work\\ref\\simple.parquet",
      "..\\..\\..\\..\\tools\\deploy\\mysql_test\\test_suite\\file_sql\\data\\simple.parquet",
  };
  for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); ++i) {
    std::ifstream in(candidates[i], std::ios::binary);
    if (in.is_open()) {
      return std::string(candidates[i]);
    }
  }
  return std::string();
}

}  // namespace

int main(int argc, char **argv) {
  std::string simple = find_simple_parquet(argc, argv);
  bool simple_is_temp = false;
  if (simple.empty()) {
    // Fall back to the official fixture embedded as base64 so the test is
    // self-contained even on a machine without D:\seekdb-work\ref.
    std::vector<u8> bytes = base64_decode(kSimpleParquetB64);
    if (!bytes.empty()) {
      simple = temp_path("official_simple.parquet");
      if (write_bytes(simple, bytes)) {
        simple_is_temp = true;
      } else {
        simple.clear();
      }
    }
  }
  if (!simple.empty()) {
    test_simple_parquet(simple);
  } else {
    std::printf("NOTE: simple.parquet not found; skipping the official fixture\n");
  }
  test_generated_types();
  test_real_arrow_fixtures();
  test_projection();
  test_errors();
  test_fuzz();
  test_empty_row_group();
  if (simple_is_temp) {
    std::remove(simple.c_str());
  }

  std::printf("\n%d checks, %d failure(s)\n", g_checks, g_failures);
  return g_failures == 0 ? 0 : 1;
}

