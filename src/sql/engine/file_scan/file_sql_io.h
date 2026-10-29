/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
/**
 * Small byte-stream helpers shared by the file-as-database readers.
 *
 * The readers cannot use std::getline() because CSV/JSONL records may contain
 * embedded newlines (quoted CSV fields) and because we must know physical line
 * numbers for diagnostics.  BufferedReader below is a plain buffered byte
 * reader with two deliberate normalizations:
 *
 *   1. "\r\n" and a lone '\r' are both reported as a single '\n'.  This makes
 *      LF, CRLF and classic-Mac files behave identically, and also normalizes
 *      the line endings that appear *inside* quoted CSV fields.
 *   2. A leading UTF-8 BOM (EF BB BF) is swallowed so that the first header
 *      name / JSON key does not start with an invisible character.
 *
 * Everything here is standard C++17; no OceanBase headers are used so that the
 * readers can be unit-tested standalone (see test/run_tests.ps1).
 */

#include <cstddef>
#include <fstream>
#include <string>
#include <vector>

namespace oceanbase {
namespace sql {
namespace filesql {

class BufferedReader {
public:
  BufferedReader() {}
  ~BufferedReader() { close(); }
  BufferedReader(const BufferedReader &) = delete;
  BufferedReader &operator=(const BufferedReader &) = delete;

  // Opens `path` in binary mode and positions the stream after a possible BOM.
  // Returns false and fills `err` when the file cannot be opened.
  bool open(const std::string &path, std::string &err) {
    close();
    in_.open(path.c_str(), std::ios::in | std::ios::binary);
    if (!in_.is_open()) {
      err = "cannot open file: " + path;
      return false;
    }
    pos_ = len_ = 0;
    pushed_.clear();
    buf_.resize(kBufSize);
    const int b0 = raw_get();
    if (b0 == 0xEF) {
      const int b1 = raw_get();
      const int b2 = raw_get();
      if (!(b1 == 0xBB && b2 == 0xBF)) {
        // Not a BOM: put the bytes back in reverse order (LIFO stack).
        push_back_raw(b2);
        push_back_raw(b1);
        push_back_raw(b0);
      }
    } else if (b0 != -1) {
      push_back_raw(b0);
    }
    return true;
  }

  void close() {
    if (in_.is_open()) {
      in_.close();
    }
    pos_ = len_ = 0;
    pushed_.clear();
  }

  // Returns the next byte (0..255) or -1 on EOF.  CR/CRLF are folded to '\n'.
  int get() {
    int c = raw_get();
    if (c == '\r') {
      if (raw_peek() == '\n') {
        raw_get();
      }
      return '\n';
    }
    return c;
  }

  // Peeks one normalized byte without consuming it; -1 on EOF.
  int peek() {
    const int c = raw_peek();
    return c == '\r' ? '\n' : c;
  }

private:
  int raw_peek() {
    if (!pushed_.empty()) {
      return pushed_.back();
    }
    if (pos_ >= len_ && !refill()) {
      return -1;
    }
    return static_cast<unsigned char>(buf_[pos_]);
  }

  int raw_get() {
    if (!pushed_.empty()) {
      const int c = pushed_.back();
      pushed_.pop_back();
      return c;
    }
    if (pos_ >= len_ && !refill()) {
      return -1;
    }
    return static_cast<unsigned char>(buf_[pos_++]);
  }

  void push_back_raw(int c) {
    if (c >= 0) {
      pushed_.push_back(c);
    }
  }

  bool refill() {
    in_.read(&buf_[0], static_cast<std::streamsize>(buf_.size()));
    len_ = static_cast<size_t>(in_.gcount());
    pos_ = 0;
    return len_ > 0;
  }

  static const size_t kBufSize = 64 * 1024;

  std::ifstream in_;
  std::vector<char> buf_;
  size_t pos_ = 0;
  size_t len_ = 0;
  std::vector<int> pushed_;
};

// Reads one physical line (without the trailing newline).
// Returns 1 when a line was produced, 0 on EOF.
inline int read_physical_line(BufferedReader &in, std::string &line) {
  line.clear();
  bool saw_any = false;
  int c = -1;
  while ((c = in.get()) != -1) {
    saw_any = true;
    if (c == '\n') {
      return 1;
    }
    line.push_back(static_cast<char>(c));
  }
  return saw_any ? 1 : 0;
}

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
