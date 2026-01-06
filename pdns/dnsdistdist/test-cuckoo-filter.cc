#ifndef BOOST_TEST_DYN_LINK
#define BOOST_TEST_DYN_LINK
#endif

#define BOOST_TEST_NO_MAIN

#include <boost/test/unit_test.hpp>

#include "generic-cache.hh"

BOOST_AUTO_TEST_SUITE(test_cuckoo_filter)

BOOST_AUTO_TEST_CASE(test_cuckoo_insert_basic)
{
  CuckooFilter cuckoo{{}};

  BOOST_CHECK(!cuckoo.contains("test"));

  cuckoo.insertKey("test");
  BOOST_CHECK(cuckoo.contains("test"));

  cuckoo.remove("test");
  BOOST_CHECK(!cuckoo.contains("test"));
}

BOOST_AUTO_TEST_CASE(test_cuckoo_insert_duplicate)
{
  CuckooFilter cuckoo{{.d_maxEntries = 10}};

  BOOST_CHECK(!cuckoo.contains("test"));
  for (size_t i = 0; i < 100; i++) {
    cuckoo.insertKey("test_" + std::to_string(i));
    BOOST_CHECK(cuckoo.contains("test_" + std::to_string(i)));
  }

  cuckoo.remove("test");
  BOOST_CHECK(!cuckoo.contains("test"));
}

#if 0

#define CSV_OUTPUT

static void printStats(const CuckooFilter& cuckoo, size_t capacity)
{
#ifdef CSV_OUTPUT
  cerr << cuckoo.getStats().d_entriesCount << ",";
  cerr << double(cuckoo.getStats().d_entriesCount) / capacity << ",";
  cerr << cuckoo.getStats().d_cacheHits << ",";
  cerr << cuckoo.getStats().d_cacheMisses << ",";
  cerr << cuckoo.getStats().d_expiredItems << ",";
  cerr << cuckoo.getStats().d_kickedItems << ",";
#else
  cerr << "Filter stats: " << endl;
  cerr << "        Entries: " << cuckoo.getStats().d_entriesCount << endl;
  cerr << "    Load factor: " << double(cuckoo.getStats().d_entriesCount) / capacity << endl;
  cerr << "           Hits: " << cuckoo.getStats().d_cacheHits << endl;
  cerr << "         Misses: " << cuckoo.getStats().d_cacheMisses << endl;
  cerr << "        Expired: " << cuckoo.getStats().d_expiredItems << endl;
  cerr << "         Kicked: " << cuckoo.getStats().d_kickedItems << endl;
  cerr << endl;
#endif
}

static size_t getActualCapacity(size_t maxEntries, size_t bucketSize)
{
  size_t bucketCount = (maxEntries + bucketSize - 1) / bucketSize;

  size_t numBuckets = 1;
  while (numBuckets < bucketCount) {
    numBuckets <<= 1;
  }
  return numBuckets * bucketSize;
}

static void runBenchmark(const std::vector<std::string>& data, CuckooFilter::CuckooSettings settings, std::function<void(CuckooFilter&)> prepopulate = {})
{
  const size_t count = data.size();
  const size_t maxEntries = settings.d_maxEntries;
  const size_t bucketSize = settings.d_bucketSize;
#ifdef CSV_OUTPUT
  cerr << count << ",";
  cerr << settings.d_maxKicks << ",";
  cerr << settings.d_maxEntries << ",";
  cerr << getActualCapacity(maxEntries, bucketSize) << ",";
  cerr << settings.d_bucketSize << ",";
  cerr << settings.d_fingerprintBits << ",";
  cerr << settings.d_ttlEnabled << ",";
  cerr << settings.d_ttl << ",";
  cerr << settings.d_ttlBits << ",";
  cerr << settings.d_ttlResolution << ",";
  cerr << settings.d_lruEnabled << ",";
#else
  cerr << "Benchmarking configuration: " << endl;
  cerr << "     Items to insert: " << count << endl;
  cerr << "           Max kicks: " << settings.d_maxKicks << endl;
  cerr << "         Max entries: " << settings.d_maxEntries << endl;
  cerr << "     Actual capacity: " << getActualCapacity(maxEntries, bucketSize) << endl;
  cerr << "         Bucket size: " << settings.d_bucketSize << endl;
  cerr << "    Fingerprint bits: " << settings.d_fingerprintBits << endl;
  cerr << "         TTL enabled: " << settings.d_ttlEnabled << endl;
  cerr << "                 TTL: " << settings.d_ttl << endl;
  cerr << "            TTL bits: " << settings.d_ttlBits << endl;
  cerr << "      TTL resolution: " << settings.d_ttlResolution << endl;
  cerr << "         LRU enabled: " << settings.d_lruEnabled << endl;
  cerr << endl;
#endif
  CuckooFilter cuckoo{settings};
  if (prepopulate) {
    prepopulate(cuckoo);
#ifndef CSV_OUTPUT
    cerr << "Prepopulating done!" << endl;
    printStats(cuckoo, getActualCapacity(maxEntries, bucketSize));
#endif
  }
#ifdef CSV_OUTPUT
  cerr << cuckoo.getStats().d_entriesCount << ",";
#endif
  DTime dt;
  dt.set();

  for (auto& item : data) {
    cuckoo.insertKey(item);
  }
  auto diff = dt.udiff();
  auto throughput = (1.0 / diff) * count;
#ifdef CSV_OUTPUT
  cerr << diff / (double(count) / 1000) << "," << throughput << ",";
#else
  cerr << "Inserting " << (count) << " items took " << diff << "μs (" << diff / (double(count) / 1000) << " ns per item). throughput: " << throughput << " M items/s." << endl;
#endif
#ifndef CSV_OUTPUT
  printStats(cuckoo, getActualCapacity(maxEntries, bucketSize));
#endif

  dt.set();
  for (auto& item : data) {
    cuckoo.contains(item);
  }
  diff = dt.udiff();
  throughput = (1.0 / diff) * count;
#ifdef CSV_OUTPUT
  cerr << diff / (double(count) / 1000) << "," << throughput << ",";
#else
  cerr << "Checking " << (count) << " items took " << diff << "μs (" << diff / (double(count) / 1000) << " ns per item). throughput: " << throughput << " M items/s." << endl;
#endif
  printStats(cuckoo, getActualCapacity(maxEntries, bucketSize));

  dt.set();
  for (auto& item : data) {
    cuckoo.remove(item);
  }
  diff = dt.udiff();
  throughput = (1.0 / diff) * count;
#ifdef CSV_OUTPUT
  cerr << diff / (double(count) / 1000) << "," << throughput << endl;
#else
  cerr << "Removing " << (count) << " items took " << diff << "μs (" << diff / (double(count) / 1000) << " ns per item). throughput: " << throughput << " M items/s." << endl;
#endif
#ifndef CSV_OUTPUT
  printStats(cuckoo, getActualCapacity(maxEntries, bucketSize));
#endif
}

BOOST_AUTO_TEST_CASE(test_cuckoo_benchmark_large_low_load)
{
  const size_t count = 1000000;
  std::vector<std::string> items{count};

  for (size_t i = 0; i < count; ++i) {
    items[i] = "test_" + std::to_string(i);
  }

  runBenchmark(items, {.d_maxEntries = 100000000, .d_bucketSize = 4, .d_fingerprintBits = 7});
  runBenchmark(items, {.d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 7});
  runBenchmark(items, {.d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 15});
  runBenchmark(items, {.d_maxEntries = 100000000, .d_bucketSize = 4, .d_fingerprintBits = 32});
  runBenchmark(items, {.d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 32});
  runBenchmark(items, {.d_maxEntries = 100000000, .d_bucketSize = 100, .d_fingerprintBits = 32});
}

BOOST_AUTO_TEST_CASE(test_cuckoo_benchmark_large_full)
{
  const size_t count = 1000000;
  std::vector<std::string> items{count};

  for (size_t i = 0; i < count; ++i) {
    items[i] = "test_" + std::to_string(i);
  }

  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 4, .d_fingerprintBits = 32}, [](CuckooFilter& filter) {
    size_t capacity = getActualCapacity(100000000, 4);
    for (size_t i = 0; i < capacity; ++i) {
      filter.insertKey("prepopulated_" + std::to_string(i));
    }
  });
  runBenchmark(items, {.d_maxKicks = 500, .d_maxEntries = 100000000, .d_bucketSize = 4, .d_fingerprintBits = 32}, [](CuckooFilter& filter) {
    size_t capacity = getActualCapacity(100000000, 4);
    for (size_t i = 0; i < capacity; ++i) {
      filter.insertKey("prepopulated_" + std::to_string(i));
    }
  });
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23}, [](CuckooFilter& filter) {
    size_t capacity = getActualCapacity(100000000, 8);
    for (size_t i = 0; i < capacity; ++i) {
      filter.insertKey("prepopulated_" + std::to_string(i));
    }
  });
}

BOOST_AUTO_TEST_CASE(test_cuckoo_benchmark_medium)
{
  const size_t count = 1000000;
  std::vector<std::string> items{count};

  for (size_t i = 0; i < count; ++i) {
    items[i] = "test_" + std::to_string(i);
  }

  runBenchmark(items, {.d_maxEntries = 1000000, .d_bucketSize = 4, .d_fingerprintBits = 7});
  runBenchmark(items, {.d_maxEntries = 1000000, .d_bucketSize = 8, .d_fingerprintBits = 7});
  runBenchmark(items, {.d_maxEntries = 1000000, .d_bucketSize = 8, .d_fingerprintBits = 15});
  runBenchmark(items, {.d_maxEntries = 1000000, .d_bucketSize = 4, .d_fingerprintBits = 32});
  runBenchmark(items, {.d_maxEntries = 1000000, .d_bucketSize = 8, .d_fingerprintBits = 32});
  runBenchmark(items, {.d_maxEntries = 1000000, .d_bucketSize = 100, .d_fingerprintBits = 32});

  runBenchmark(items, {.d_maxKicks = 0, .d_maxEntries = 100, .d_bucketSize = 4, .d_fingerprintBits = 8, .d_lruEnabled = true});
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23});
  runBenchmark(items, {.d_maxKicks = 0, .d_maxEntries = 100, .d_bucketSize = 4, .d_fingerprintBits = 8});
  runBenchmark(items, {.d_maxKicks = 50, .d_maxEntries = 100000, .d_bucketSize = 4, .d_fingerprintBits = 23, .d_lruEnabled = true});
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23, .d_ttlEnabled = true, .d_ttl = 1});
}

BOOST_AUTO_TEST_CASE(test_cuckoo_benchmark_small)
{
  const size_t count = 1000000;
  std::vector<std::string> items{count};

  for (size_t i = 0; i < count; ++i) {
    items[i] = "test_" + std::to_string(i);
  }

  runBenchmark(items, {.d_maxEntries = 10000, .d_bucketSize = 4, .d_fingerprintBits = 7});
  runBenchmark(items, {.d_maxEntries = 10000, .d_bucketSize = 8, .d_fingerprintBits = 7});
  runBenchmark(items, {.d_maxEntries = 10000, .d_bucketSize = 8, .d_fingerprintBits = 15});
  runBenchmark(items, {.d_maxEntries = 10000, .d_bucketSize = 4, .d_fingerprintBits = 32});
  runBenchmark(items, {.d_maxEntries = 10000, .d_bucketSize = 8, .d_fingerprintBits = 32});
  runBenchmark(items, {.d_maxEntries = 10000, .d_bucketSize = 100, .d_fingerprintBits = 32});

  runBenchmark(items, {.d_maxKicks = 0, .d_maxEntries = 100, .d_bucketSize = 4, .d_fingerprintBits = 8, .d_lruEnabled = true});
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23});
  runBenchmark(items, {.d_maxKicks = 0, .d_maxEntries = 100, .d_bucketSize = 4, .d_fingerprintBits = 8});
  runBenchmark(items, {.d_maxKicks = 50, .d_maxEntries = 100000, .d_bucketSize = 4, .d_fingerprintBits = 23, .d_lruEnabled = true});
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23, .d_ttlEnabled = true, .d_ttl = 1});
}

// TODO: reogranize this one
BOOST_AUTO_TEST_CASE(test_cuckoo_benchmark_other)
{
  const size_t count = 1000000;
  std::vector<std::string> items{count};

  for (size_t i = 0; i < count; ++i) {
    items[i] = "test_" + std::to_string(i);
  }

  runBenchmark(items, {.d_maxKicks = 0, .d_maxEntries = 100, .d_bucketSize = 4, .d_fingerprintBits = 8, .d_lruEnabled = true});
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23});
  runBenchmark(items, {.d_maxKicks = 0, .d_maxEntries = 100, .d_bucketSize = 4, .d_fingerprintBits = 8});
  runBenchmark(items, {.d_maxKicks = 50, .d_maxEntries = 100000, .d_bucketSize = 4, .d_fingerprintBits = 23, .d_lruEnabled = true});
  runBenchmark(items, {.d_maxKicks = 8, .d_maxEntries = 100000000, .d_bucketSize = 8, .d_fingerprintBits = 23, .d_ttlEnabled = true, .d_ttl = 1});
}

#endif
BOOST_AUTO_TEST_SUITE_END()
