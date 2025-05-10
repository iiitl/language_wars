// parallel_sort_mmap_fast.cpp
// Blazing‐fast sort of 1B uints using mmap, parallel sort, and bulk write with to_chars.

#include <bits/stdc++.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <charconv>
#include <parallel/algorithm>
using namespace std;

// 1) Phast as phreek mmap-based loader
vector<bool> load_strings(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) throw runtime_error(string("open failed: ") + path);
    struct stat st;
    if (fstat(fd, &st) < 0) throw runtime_error("fstat failed");
    size_t sz = st.st_size;
    char* data = (char*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) throw runtime_error("mmap failed");
    vector<bool> a(100000000, 0);
    string current;
    for (size_t i = 0; i < sz; ++i) {
      char c = data[i];
      if (c == '\n') {
        if (!current.empty()) {
          int id = 0;
          for(int i = current.size() - 1; i >= 0; i--) {
              id *= 26;
              id += current[i] - 'a' + 1;
          }
          a[id] = 1;
          current.clear();
        }
      } else {
        current += c;
      }
    }
    munmap(data, sz);
    close(fd);
    return a;
}

// 2) Bulk writer: writes indices of true values in the bool array as base26 words
string index_to_base26(int index) {
  string result;
  while (index > 0) {
    int remainder = (index - 1) % 26;
    result += remainder + 'a';
    index = (index - 1) / 26;
  }
  // reverse(result.begin(), result.end());
  return result;
}

void write_indices_as_words(const char* path, const vector<bool>& a) {
  int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) throw runtime_error(string("open failed: ") + path);

  const size_t BUF_SZ = 100 * 1024 * 1024;
  vector<char> buf;
  buf.reserve(BUF_SZ);

  for (size_t i = 0, n = a.size(); i < n; ++i) {
    if (a[i]) {
      string word = index_to_base26(i);
      // reverse(word.begin(), word.end()); // Reverse the string to correct the order
      size_t len = word.size();
      // flush if not enough room (word + newline)
      if (buf.size() + len + 1 > BUF_SZ) {
        write(fd, buf.data(), buf.size());
        buf.clear();
      }
      // append word
      buf.insert(buf.end(), word.begin(), word.end());
      // append newline
      buf.push_back('\n');
    }
  }
  if (!buf.empty())
    write(fd, buf.data(), buf.size());
  close(fd);
}
void write_indices_fast(const char* path, const vector<bool>& a) {
  int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) throw runtime_error(string("open failed: ") + path);

  const size_t BUF_SZ = 100 * 1024 * 1024;
  vector<char> buf;
  buf.reserve(BUF_SZ);

  for (size_t i = 0, n = a.size(); i < n; ++i) {
    if (a[i]) {
      // format index to temp buffer
      char tmp[12];
      auto [ptr, ec] = to_chars(tmp, tmp + sizeof(tmp), i);
      size_t len = ptr - tmp;
      // flush if not enough room (number + newline)
      if (buf.size() + len + 1 > BUF_SZ) {
        write(fd, buf.data(), buf.size());
        buf.clear();
      }
      // append digits
      buf.insert(buf.end(), tmp, tmp + len);
      // append newline
      buf.push_back('\n');
    }
  }
  if (!buf.empty())
    write(fd, buf.data(), buf.size());
  close(fd);
}
void write_uints_fast(const char* path, const vector<uint32_t>& a) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) throw runtime_error(string("open failed: ") + path);

    const size_t BUF_SZ = 100 * 1024 * 1024;
    vector<char> buf;
    buf.reserve(BUF_SZ);

    for (size_t i = 0, n = a.size(); i < n; ++i) {
        // format number to temp buffer
        char tmp[12];
        auto [ptr, ec] = to_chars(tmp, tmp + sizeof(tmp), a[i]);
        size_t len = ptr - tmp;
        // flush if not enough room (number + space)
        if (buf.size() + len + 1 > BUF_SZ) {
            write(fd, buf.data(), buf.size());
            buf.clear();
        }
        // append digits
        buf.insert(buf.end(), tmp, tmp + len);
        // append space if not last
        if (i + 1 < n) buf.push_back(' ');
    }
    if (!buf.empty())
        write(fd, buf.data(), buf.size());
    close(fd);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0]
             << " <input.txt> <output.txt>\n";
        return 1;
    }
    const char* in_path  = argv[1];
    const char* out_path = argv[2];

    auto t0 = chrono::high_resolution_clock::now();

    // Load

    auto a = load_strings(in_path);
    size_t n = a.size();
    // cerr << "Loaded " << n << " bo into RAM ("
    //      << (n * sizeof(uint32_t)) / (1024*1024)
    //      << " MiB)\n";

    int res = 0;
    for(auto it: a){
      res += it;
    }
    cout << res << endl;
    // // Parallel sort
    // __gnu_parallel::sort(a.begin(), a.end());
    // auto t1 = chrono::high_resolution_clock::now();
    // chrono::duration<double> sort_time = t1 - t0;
    // cerr << "Parallel sort done in "
    //      << sort_time.count() << " s\n";

    // // Fast write
    // write_uints_fast(out_path, a);
    // auto t2 = chrono::high_resolution_clock::now();
    // chrono::duration<double> total = t2 - t0;
    // cerr << "Write complete. Total elapsed: "
    //      << total.count() << " s\n";
    write_indices_as_words(out_path, a);
    // auto t2 = chrono::high_resolution_clock::now(); 
    return 0;
}
