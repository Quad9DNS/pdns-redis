/*
 * This file is part of PowerDNS or dnsdist.
 * Copyright -- PowerDNS.COM B.V. and its contributors
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * In addition, for the avoidance of any doubt, permission is granted to
 * link this program with OpenSSL and to (re)distribute the binaries
 * produced as the result of such linking.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
#pragma once

#include <atomic>
#include <boost/dynamic_bitset.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/key_extractors.hpp>
#include <iterator>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <vector>
#include <random>

#include "cachecleaner.hh"
#include "gettime.hh"
#include "lock.hh"
#include "noinitvector.hh"
#include "ext/probds/murmur3.h"
#include "stable-bloom.hh"

using namespace ::boost::multi_index;

class GenericExpiringCacheInterface
{
public:
  virtual ~GenericExpiringCacheInterface() {};
  virtual size_t purgeExpired(size_t upTo, time_t now) = 0;
  virtual size_t expunge(size_t upTo = 0) = 0;
};

template <typename K>
class GenericFilterInterface : public GenericExpiringCacheInterface
{
public:
  virtual ~GenericFilterInterface() {};
  virtual void insertKey(const K& key) = 0;
  virtual bool contains(const K& key) = 0;
};

template <typename K, typename V>
class GenericCacheInterface : public GenericFilterInterface<K>
{
public:
  virtual ~GenericCacheInterface() {};
  virtual void insert(const K& key, V value) = 0;
  virtual bool getValue(const K& key, V& value) = 0;
};

template <typename K, typename V, typename Hash = std::hash<K>>
class GenericCache : public GenericCacheInterface<K, V>, boost::noncopyable
{
private:
  struct CacheValue
  {
    K key;
    V value;
    time_t validity;
  };

public:
  struct CacheSettings
  {
    bool d_ttlEnabled;
    unsigned int d_ttl;
    bool d_lruEnabled;
    uint32_t d_shardCount{1};
    uint32_t d_maxEntries{0};
    uint32_t d_lruDeleteUpTo{0};
  };

  GenericCache(CacheSettings settings) :
    d_settings(settings), d_shards(settings.d_shardCount)
  {
  }
  virtual ~GenericCache() {};

  void insert(const K& key, V value) override
  {
    size_t hash = Hash{}(key);
    size_t shardIndex = hash % d_settings.d_shardCount;

    if (d_settings.d_maxEntries > 0 && d_shards.at(shardIndex).d_entriesCount >= (d_settings.d_maxEntries / d_settings.d_shardCount)) {
      if (d_settings.d_ttlEnabled) {
        timespec now;
        gettime(&now);
        purgeExpired(0, now.tv_sec);
      }
      if (d_settings.d_lruEnabled) {
        expunge(d_settings.d_lruDeleteUpTo == 0 ? d_settings.d_maxEntries - 1 : d_settings.d_lruDeleteUpTo);
      }

      if (d_shards.at(shardIndex).d_entriesCount >= (d_settings.d_maxEntries / d_settings.d_shardCount)) {
        return;
      }
    }

    time_t validity;
    if (d_settings.d_ttlEnabled) {
      timespec now;
      gettime(&now);
      validity = now.tv_sec + d_settings.d_ttl;
    }
    else {
      validity = time_t();
    }
    CacheValue cacheValue{
      .key = key,
      .value = value,
      .validity = validity};

    auto& shard = d_shards.at(shardIndex);

    auto map = shard.d_map.write_lock();

    // check again now that we hold the lock to prevent a race
    if (d_settings.d_maxEntries > 0 && map->size() >= (d_settings.d_maxEntries / d_settings.d_shardCount)) {
      return;
    }

    auto result = map->insert(cacheValue);

    // TODO memory usage calculation here?
    if (!result.second) {
      if (map->replace(result.first, cacheValue)) {
        ++shard.d_entriesCount;
      }
    }
    else {
      ++shard.d_entriesCount;
    }
  }

  void insertKey(const K& key) override
  {
    if constexpr (std::is_default_constructible<V>()) {
      insert(key, V());
    }
    else {
      throw new std::runtime_error("Unsupported insertKey operation.");
    }
  }

  bool getValue(const K& key, V& value) override
  {
    size_t hash = Hash{}(key);
    size_t shardIndex = hash % d_settings.d_shardCount;
    auto result = false;
    auto& shard = d_shards.at(shardIndex);
    {
      auto map = shard.d_map.read_lock();

      auto mapIt = map->find(key);
      if (mapIt == map->end()) {
        return false;
      }

      if (d_settings.d_ttlEnabled) {
        timespec now;
        gettime(&now);
        if (mapIt->validity > now.tv_sec) {
          value = mapIt->value;
          result = true;
        }
      }
      else {
        value = mapIt->value;
        result = true;
      }
    }

    if (d_settings.d_lruEnabled || (!result && d_settings.d_ttlEnabled)) {
      auto map = shard.d_map.write_lock();
      auto mapIt = map->find(key);
      if (mapIt == map->end()) {
        return result;
      }
      if (d_settings.d_lruEnabled) {
        moveCacheItemToBack<SequencedTag>(*map, mapIt);
      }
      if (!result && d_settings.d_ttlEnabled) {
        map->erase(mapIt);
      }
    }

    return result;
  }

  bool contains(const K& key) override
  {
    size_t hash = Hash{}(key);
    size_t shardIndex = hash % d_settings.d_shardCount;
    auto result = false;
    auto& shard = d_shards.at(shardIndex);
    {
      auto map = shard.d_map.read_lock();

      auto mapIt = map->find(key);

      if (mapIt == map->end()) {
        return false;
      }

      if (d_settings.d_ttlEnabled) {
        timespec now;
        gettime(&now);
        if (mapIt->validity > now.tv_sec) {
          result = true;
        }
      }
      else {
        result = true;
      }
    }

    if (d_settings.d_lruEnabled || (!result && d_settings.d_ttlEnabled)) {
      auto map = shard.d_map.write_lock();
      auto mapIt = map->find(key);
      if (mapIt == map->end()) {
        return result;
      }
      if (d_settings.d_lruEnabled) {
        moveCacheItemToBack<SequencedTag>(*map, mapIt);
      }
      if (!result && d_settings.d_ttlEnabled) {
        map->erase(mapIt);
      }
    }

    return result;
  }

  size_t purgeExpired(size_t upTo, const time_t now) override
  {
    if (d_settings.d_ttlEnabled) {
      const size_t maxPerShard = upTo / d_settings.d_shardCount;

      size_t removed = 0;
      for (auto& shard : d_shards) {
        auto map = shard.d_map.write_lock();
        if (map->size() <= maxPerShard) {
          continue;
        }

        size_t toRemove = map->size() - maxPerShard;

        for (auto it = map->begin(); toRemove > 0 && it != map->end();) {
          if (it->validity <= now) {
            it = map->erase(it);
            --toRemove;
            --shard.d_entriesCount;
            ++removed;
          }
          else {
            ++it;
          }
        }
      }

      return removed;
    }
    else {
      return expunge(upTo);
    }
  }

  size_t expunge(size_t upTo = 0) override
  {
    const size_t maxPerShard = upTo / d_settings.d_shardCount;

    size_t removed = 0;

    for (auto& shard : d_shards) {
      auto map = shard.d_map.write_lock();

      if (map->size() <= maxPerShard) {
        continue;
      }

      size_t toRemove = map->size() - maxPerShard;

      if (map->size() >= toRemove) {
        auto& sequence = map->template get<SequencedTag>();
        auto beginIt = sequence.begin();
        auto endIt = beginIt;

        std::advance(endIt, toRemove);
        sequence.erase(beginIt, endIt);
        shard.d_entriesCount -= toRemove;
        removed += toRemove;
      }
      else {
        removed += map->size();
        map->clear();
        shard.d_entriesCount = 0;
      }
    }

    return removed;
  }

private:
  struct HashedTag
  {
  };
  struct SequencedTag
  {
  };

  class CacheShard
  {
  public:
    CacheShard()
    {
    }
    CacheShard(const CacheShard& /* old */)
    {
    }

    void setSize(size_t maxSize)
    {
      d_map.write_lock()->reserve(maxSize);
    }

    using cache_t = multi_index_container<
      CacheValue,
      indexed_by<
        hashed_unique<tag<HashedTag>, member<CacheValue, K, &CacheValue::key>, Hash>,
        sequenced<tag<SequencedTag>>>>;

    SharedLockGuarded<cache_t> d_map;
    std::atomic<uint64_t> d_entriesCount{0};
  };

  CacheSettings d_settings;
  std::vector<CacheShard> d_shards;
};

class BloomFilter : public GenericCacheInterface<std::string, std::string>
{
public:
  struct BloomSettings
  {
    float d_fpRate{0.01};
    size_t d_numCells{67108864};
    size_t d_numDec{10};
  };

  BloomFilter(BloomSettings settings) :
    d_settings(settings), d_sbf(bf::stableBF(settings.d_fpRate, settings.d_numCells, settings.d_numDec))
  {
  }

  virtual ~BloomFilter() {};

  void insertKey(const std::string& key) override
  {
    d_sbf.lock()->add(key);
  }

  void insert(const std::string& key, [[maybe_unused]] std::string value) override
  {
    insertKey(key);
  }

  bool contains(const std::string& key) override
  {
    return d_sbf.lock()->test(key);
  }

  bool getValue(const std::string& key, [[maybe_unused]] std::string& value) override
  {
    return contains(key);
  }

  size_t purgeExpired([[maybe_unused]] size_t upTo, [[maybe_unused]] time_t now) override
  {
    // Unsupported
    return 0;
  }

  size_t expunge([[maybe_unused]] size_t upTo = 0) override
  {
    // Unsupported
    return 0;
  }

private:
  BloomSettings d_settings;
  LockGuarded<bf::stableBF> d_sbf;
};

class CuckooFilter : public GenericCacheInterface<std::string, std::string>
{
  static constexpr size_t BUCKET_SIZE = 4;
  static constexpr size_t FINGERPRINT_BITS = 8;

public:
  struct CuckooSettings
  {
    unsigned int d_maxKicks{500};
    uint32_t d_maxEntries{100000};
    bool d_ttlEnabled;
    unsigned int d_ttl;
    bool d_lruEnabled;
  };

  CuckooFilter(CuckooSettings settings) :
    d_settings(settings), d_numBuckets(getBucketCount(settings.d_maxEntries)), d_numBucketsMask(d_numBuckets - 1), d_buckets(d_numBuckets)
  {
  }

  virtual ~CuckooFilter() {};

  void insertKey(const std::string& key) override
  {
    auto [i1, i2, fp] = get_indices_and_fingerprint(key);

    // Try optimistic insert in both buckets
    if (d_buckets[i1].try_insert_optimistic(fp) || d_buckets[i2].try_insert_optimistic(fp)) {
      return;
    }

    // Try with locks
    if (d_buckets[i1].insert_with_lock(fp) || d_buckets[i2].insert_with_lock(fp)) {
      return;
    }

    // Cuckoo eviction
    size_t cur_index = i1;
    Fingerprint cur_fp = fp;
    uint8_t cur_counter = 0;

    for (size_t kick = 0; kick < d_settings.d_maxKicks; ++kick) {
      if (d_settings.d_lruEnabled) {
        bool kicked = d_buckets[cur_index].kick_lru(cur_fp, cur_fp, cur_counter, cur_counter);
        if (!kicked) {
          return;
        }
      }
      else {
        cur_fp = d_buckets[cur_index].kick_random(cur_fp, d_gen);
      }
      cur_index = alt_index(cur_index, cur_fp);

      if (d_buckets[cur_index].try_insert_optimistic(cur_fp) || d_buckets[cur_index].insert_with_lock(cur_fp)) {
        // TODO: counter is lost here and reset to 1 - probably not good
        return;
      }
    }

    return; // Filter is full
  }

  void insert(const std::string& key, [[maybe_unused]] std::string value) override
  {
    insertKey(key);
  }

  bool contains(const std::string& key) override
  {
    auto [i1, i2, fp] = get_indices_and_fingerprint(key);

    return d_buckets[i1].contains(fp) || d_buckets[i2].contains(fp);
  }

  bool getValue(const std::string& key, [[maybe_unused]] std::string& value) override
  {
    return contains(key);
  }

  size_t purgeExpired([[maybe_unused]] size_t upTo, [[maybe_unused]] time_t now) override
  {
    // TODO
    return 0;
  }

  size_t expunge([[maybe_unused]] size_t upTo = 0) override
  {
    // TODO
    return 0;
  }

private:
  using Fingerprint = uint8_t;

  static constexpr Fingerprint EMPTY_FINGERPRINT = 0;
  static constexpr Fingerprint FINGERPRINT_MASK = (1 << FINGERPRINT_BITS) - 1;

  static size_t getBucketCount(size_t maxEntries)
  {
    size_t bucketCount = (maxEntries + BUCKET_SIZE - 1) / BUCKET_SIZE;

    size_t numBuckets = 1;
    while (numBuckets < bucketCount) {
      numBuckets <<= 1;
    }
    return numBuckets;
  }

  struct Bucket
  {
    std::atomic<Fingerprint> fingerprints[BUCKET_SIZE];
    std::atomic<uint8_t> counters[BUCKET_SIZE];
    mutable std::shared_mutex mutex; // Fine-grained locking per bucket

    Bucket()
    {
      for (size_t i = 0; i < BUCKET_SIZE; ++i) {
        fingerprints[i].store(EMPTY_FINGERPRINT, std::memory_order_relaxed);
        counters[i].store(0, std::memory_order_relaxed);
      }
    }

    // Try to insert without lock (optimistic)
    bool try_insert_optimistic(Fingerprint fp)
    {
      for (size_t i = 0; i < BUCKET_SIZE; ++i) {
        Fingerprint expected = EMPTY_FINGERPRINT;
        if (fingerprints[i].compare_exchange_weak(expected, fp,
                                                  std::memory_order_acq_rel, std::memory_order_acquire)) {
          counters[i].store(1, std::memory_order_release);
          return true;
        }
      }
      return false;
    }

    // Insert with shared lock
    bool insert_with_lock(Fingerprint fp)
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      for (size_t i = 0; i < BUCKET_SIZE; ++i) {
        if (fingerprints[i].load(std::memory_order_acquire) == EMPTY_FINGERPRINT) {
          fingerprints[i].store(fp, std::memory_order_release);
          counters[i].store(1, std::memory_order_release);
          return true;
        }
      }
      return false;
    }

    // Lookup with minimal locking
    bool contains(Fingerprint fp)
    {
      // First try optimistic read
      for (size_t i = 0; i < BUCKET_SIZE; ++i) {
        if (fingerprints[i].load(std::memory_order_acquire) == fp) {
          counters[i].fetch_add(1, std::memory_order_relaxed);
          return true;
        }
      }
      return false;
    }

    // Remove with lock
    bool remove(Fingerprint fp)
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      for (size_t i = 0; i < BUCKET_SIZE; ++i) {
        if (fingerprints[i].load(std::memory_order_acquire) == fp) {
          fingerprints[i].store(EMPTY_FINGERPRINT, std::memory_order_release);
          counters[i].store(0, std::memory_order_release);
          return true;
        }
      }
      return false;
    }

    // For cuckoo eviction - returns evicted fingerprint
    Fingerprint kick_random(Fingerprint new_fp, std::mt19937& rng)
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      size_t pos = rng() % BUCKET_SIZE;
      Fingerprint old_fp = fingerprints[pos].exchange(new_fp, std::memory_order_acq_rel);
      return old_fp;
    }

    // For LRU cuckoo eviction - returns true if fingeprint was evicted - the fingeprint is stored in kicked_fp
    bool kick_lru(Fingerprint new_fp, Fingerprint& kicked_fp, uint8_t newCounter, uint8_t& counter)
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      uint8_t min = newCounter;
      size_t pos = BUCKET_SIZE;
      for (size_t i = 0; i < BUCKET_SIZE; ++i) {
        uint8_t current = counters[i].load(std::memory_order_acquire);
        if (current < min) {
          pos = i;
        }
      }
      if (pos < BUCKET_SIZE) {
        counter = counters[pos].exchange(newCounter, std::memory_order_acq_rel);
      }
      else {
        return false;
      }
      Fingerprint old_fp = fingerprints[pos].exchange(new_fp, std::memory_order_acq_rel);
      kicked_fp = old_fp;
      return true;
    }

    void access_slot(int slot)
    {
      if (counters[slot].load(std::memory_order_acquire) == 255) {
        // Age all counters when one saturates
        for (int i = 0; i < 4; i++) {
          counters[i].store(counters[i].load(std::memory_order_acquire) >> 1, std::memory_order_release);
        }
      }
      counters[slot].fetch_add(1, std::memory_order_relaxed);
    }
  };

  static constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
  static constexpr uint64_t FNV_PRIME = 1099511628211ULL;

  static uint64_t fnv1a_hash(const std::string& data)
  {
    uint64_t hash = FNV_OFFSET_BASIS;
    const char* bytes = data.data();
    for (size_t i = 0; i < data.length(); ++i) {
      hash ^= bytes[i];
      hash *= FNV_PRIME;
    }
    return hash;
  }

  // Hash and fingerprint calculation
  std::tuple<size_t, size_t, Fingerprint> get_indices_and_fingerprint(const std::string& data) const
  {
    uint64_t hash = fnv1a_hash(data);
    uint32_t fingerprint_raw = murmur_hash(data);

    Fingerprint fp = static_cast<Fingerprint>((fingerprint_raw & FINGERPRINT_MASK));
    if (fp == EMPTY_FINGERPRINT)
      fp = 1; // Avoid empty fingerprint

    uint32_t bucket = murmur_hash(std::to_string(fp), 1);
    size_t i1 = hash & d_numBucketsMask;
    size_t i2 = (i1 ^ (bucket & d_numBucketsMask)) & d_numBucketsMask;

    return {i1, i2, fp};
  }

  uint32_t murmur_hash(const std::string& data, const uint32_t seed = 0x9747b28c) const
  {
    uint32_t hash{};
    // MurmurHash3 assumes the data is uint32_t aligned, so fixup if needed
    // It does handle string lengths that are not a multiple of sizeof(uint32_t) correctly
    if (reinterpret_cast<uintptr_t>(data.data()) % sizeof(uint32_t) != 0) { // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      NoInitVector<uint32_t> vec((data.length() / sizeof(uint32_t)) + 1);
      memcpy(vec.data(), data.data(), data.length());
      MurmurHash3_x86_32(vec.data(), static_cast<int>(data.length()), seed, &hash);
    }
    else {
      MurmurHash3_x86_32(data.data(), static_cast<int>(data.length()), seed, &hash);
    }

    return hash;
  }

  size_t alt_index(size_t index, Fingerprint fp) const
  {
    uint32_t bucket = murmur_hash(std::to_string(fp), 1);
    return (index ^ (bucket & d_numBucketsMask)) & d_numBucketsMask;
  }

  CuckooSettings d_settings;
  size_t d_numBuckets;
  size_t d_numBucketsMask;
  std::vector<Bucket> d_buckets;
  std::mt19937 d_gen;
};
