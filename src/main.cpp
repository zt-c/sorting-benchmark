#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <parquet/arrow/reader.h>

#include <benchmark/benchmark.h>

#include "utils.h"

#include "kxsort/kxsort.h"
#include "ska_sort/ska_sort.hpp"

#define SORT_TWO_KEY false

const std::string LINEITEM_DATA_URI = "file:///mnt/s4/sort_data/lineitem";
const std::string LINEITEM_PART_DATA_URI = "file:///mnt/s4/sort_data/lineitem_part";

const std::string SAMPLE_LINEITEM_DATA_URI = "file:///home/shelton/data/sort_data/lineitem";
const std::string SAMPLE_LINEITEM_PART_DATA_URI = "file:///home/shelton/data/sort_data/lineitem_part";

class Sorter {
 private:
  const std::string DATASET_URI = SAMPLE_LINEITEM_PART_DATA_URI;

  arrow::MemoryPool *pool_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::string dataset_dir_;
  std::vector<arrow::fs::FileInfo> file_infos_;
  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files_;

  struct ValueIndex {
    size_t array_id, array_index;

    std::string ToString() {
      char s[40];
      std::sprintf(s, "{array_id = %ld, array_index = %ld}", array_id,
                   array_index);
      return std::string(s);
    }
  };

 public:
  static bool has_shown_info_;

  std::vector<std::vector<std::shared_ptr<arrow::Int64Array>>> data_;
  std::vector<ValueIndex> orderings_;

  inline int64_t ValueAt(size_t column_id, const ValueIndex &value_index) {
    return data_[column_id][value_index.array_id]->GetView(
        value_index.array_index);
  }

  inline int64_t ValueAt(size_t column_id, size_t idx) {
    return ValueAt(column_id, orderings_[idx]);
  }

  Sorter() {
    pool_ = arrow::default_memory_pool();
    fs_ = arrow::fs::FileSystemFromUri(DATASET_URI, &dataset_dir_).ValueOrDie();

    arrow::fs::FileSelector dataset_dir_selector;
    dataset_dir_selector.base_dir = dataset_dir_;
    dataset_dir_selector.recursive = true;

    file_infos_ = fs_->GetFileInfo(dataset_dir_selector).ValueOrDie();

    for (const auto &file_info : file_infos_) {
      if (file_info.IsDirectory()) continue;
      auto file = fs_->OpenInputFile(file_info.path()).ValueOrDie();
      files_.push_back(file);
    }
  }

  void Init() {
    InitData();
    InitOrderings();
    if (!has_shown_info_) {
      char s[40];
      std::sprintf(s, "[ data ready: %ld entries ]", orderings_.size());
      print(std::string(s));
      has_shown_info_ = true;
    }
  }

  void InitData() {
    data_.resize(4);
    // data_.resize(16);
    for (auto file : files_) {
      std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
      assert(parquet::arrow::OpenFile(file, pool_, &parquet_reader).ok());

      std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
      // std::vector<int> column_indices = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
      std::vector<int> column_indices = {0, 1, 2, 3};
      assert(
          parquet_reader
              ->GetRecordBatchReader({0}, column_indices, &record_batch_reader)
              .ok());

      std::shared_ptr<arrow::RecordBatch> record_batch;
      for (;;) {
        assert(record_batch_reader->ReadNext(&record_batch).ok());
        if (!record_batch) break;
        std::vector<std::shared_ptr<arrow::Array>> raw_columns =
            record_batch->columns();
        for (size_t column_id = 0; column_id != raw_columns.size();
             ++column_id) {
          data_[column_id].emplace_back(
              std::dynamic_pointer_cast<arrow::Int64Array>(
                  raw_columns[column_id]));
        }
      }
    }
  }

  void InitOrderings() {
    orderings_.reserve(data_[0].size() * data_[0][0]->length());

    for (size_t array_id = 0; array_id != data_[0].size(); ++array_id) {
      for (size_t array_index = 0; array_index != data_[0][array_id]->length();
           ++array_index) {
        orderings_.emplace_back(ValueIndex({array_id, array_index}));
      }
    }
  }

  void stdSort() {
    std::sort(orderings_.begin(), orderings_.end(),
              [this](ValueIndex lhs, ValueIndex rhs) {
                return ValueAt(0, lhs) > ValueAt(0, rhs);
              });
  }

  void stdStableSort() {
    std::stable_sort(orderings_.begin(), orderings_.end(),
                     [this](ValueIndex lhs, ValueIndex rhs) {
                       return ValueAt(0, lhs) > ValueAt(0, rhs);
                     });
  }

  void skaSort() {
    ska_sort(
        orderings_.begin(), orderings_.end(),
        [this](ValueIndex value_index) { return ValueAt(0, value_index); });
  }

#if SORT_TWO_KEY

  void stdSort2keyV1() {
    std::sort(orderings_.begin(), orderings_.end(),
              [this](ValueIndex lhs, ValueIndex rhs) {
                return (static_cast<__int128_t>(ValueAt(0, lhs)) << 64) +
                           ValueAt(1, lhs) <
                       (static_cast<__int128_t>(ValueAt(0, rhs)) << 64) +
                           ValueAt(1, rhs);
                // return (static_cast<int64_t>(ValueAt(0, lhs)) << 32) +
                //            ValueAt(1, lhs) <
                //        (static_cast<int64_t>(ValueAt(0, rhs)) << 32) +
                //            ValueAt(1, rhs);
              });
  }



  void stdSort2keyV2() {
    std::sort(orderings_.begin(), orderings_.end(),
              [this](ValueIndex lhs, ValueIndex rhs) {
                return (ValueAt(0, lhs) > ValueAt(0, rhs)) || (ValueAt(0, lhs) == ValueAt(0, rhs) && ValueAt(1, lhs) > ValueAt(1, rhs));
              });
  }

  void stdStableSort2KeyV1() {
    std::stable_sort(
        orderings_.begin(), orderings_.end(),
        [this](ValueIndex lhs, ValueIndex rhs) {
          __int128_t l_val =
              (static_cast<__int128_t>(ValueAt(0, lhs)) << 64) + ValueAt(1, lhs);
          __int128_t r_val =
              (static_cast<__int128_t>(ValueAt(0, rhs)) << 64) + ValueAt(1, rhs);
          // int64_t l_val =
          //     (static_cast<int64_t>(ValueAt(0, lhs)) << 32) + ValueAt(1, lhs);
          // int64_t r_val =
          //     (static_cast<int64_t>(ValueAt(0, rhs)) << 32) + ValueAt(1, rhs);
          return l_val < r_val;
        });
  }

  void stdStableSort2KeyV2() {
    std::stable_sort(orderings_.begin(), orderings_.end(),
                     [this](ValueIndex lhs, ValueIndex rhs) {
                       if (ValueAt(0, lhs) > ValueAt(0, rhs)) {
                         return true;
                       } else if (ValueAt(0, lhs) == ValueAt(0, rhs) &&
                                  ValueAt(1, lhs) > ValueAt(1, rhs)) {
                         return true;
                       } else {
                         return false;
                       }
                     });
  }

  void skaSort2Key() {
    ska_sort(orderings_.begin(), orderings_.end(),
             [this](ValueIndex value_index) {
               return (static_cast<__int128_t>(ValueAt(0, value_index)) << 64) +
                      ValueAt(1, value_index);
              //  return (static_cast<int64_t>(ValueAt(0, value_index)) << 32) +
              //         ValueAt(1, value_index);
             });
  }
#endif
};

bool Sorter::has_shown_info_ = false;

void BM_InitData(benchmark::State &state) {
  Sorter sorter;
  for (auto _ : state) {
    sorter.InitData();
  }
}
BENCHMARK(BM_InitData)->Unit(benchmark::kMillisecond);

void BM_InitOrderings(benchmark::State &state) {
  Sorter sorter;
  sorter.InitData();
  for (auto _ : state) {
    sorter.InitOrderings();
  }
}
BENCHMARK(BM_InitOrderings)->Unit(benchmark::kMillisecond);

void BM_StdSort(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdSort();
  }
}
BENCHMARK(BM_StdSort)->Unit(benchmark::kMillisecond);

void BM_StdStableSort(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdStableSort();
  }
}
BENCHMARK(BM_StdStableSort)->Unit(benchmark::kMillisecond);

void BM_SkaSort(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.skaSort();
  }
}
BENCHMARK(BM_SkaSort)->Unit(benchmark::kMillisecond);


#if SORT_TWO_KEY

void BM_StdSort2KeyV1(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdSort2keyV1();
  }
}
BENCHMARK(BM_StdSort2KeyV1)->Unit(benchmark::kMillisecond);

void BM_StdSort2KeyV2(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdSort2keyV2();
  }
}
BENCHMARK(BM_StdSort2KeyV2)->Unit(benchmark::kMillisecond);

void BM_StdStableSort2KeyV1(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdStableSort2KeyV1();
  }
}
BENCHMARK(BM_StdStableSort2KeyV1)->Unit(benchmark::kMillisecond);

void BM_StdStableSort2KeyV2(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdStableSort2KeyV2();
  }
}
BENCHMARK(BM_StdStableSort2KeyV2)->Unit(benchmark::kMillisecond);

void BM_SkaSort2Key(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.skaSort2Key();
  }
}
BENCHMARK(BM_SkaSort2Key)->Unit(benchmark::kMillisecond);

#endif

BENCHMARK_MAIN();
