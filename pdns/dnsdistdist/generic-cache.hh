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

#include <algorithm>
#include <atomic>
#include <boost/dynamic_bitset.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/key_extractors.hpp>
#include <cstring>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <random>

#include "cachecleaner.hh"
#include "generic-cache-interface.hh"
#include "gettime.hh"
#include "lock.hh"
#include "ext/probds/murmur3.h"
#include "stable-bloom.hh"

using namespace ::boost::multi_index;

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
    d_stats.d_memoryUsed = sizeof(*this) + d_shards.size() * sizeof(CacheShard);
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

    if (!result.second) {
      // This key already exists - replace it
      if (map->replace(result.first, cacheValue)) {
        d_stats.d_memoryUsed += sizeof(cacheValue) - sizeof(result.first);
      }
    }
    else {
      ++d_stats.d_entriesCount;
      ++shard.d_entriesCount;
      d_stats.d_memoryUsed += sizeof(cacheValue);
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
        d_stats.d_cacheMisses += 1;
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

    if (result) {
      d_stats.d_cacheHits += 1;
    }
    else {
      d_stats.d_cacheMisses += 1;
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
        shard.d_entriesCount -= 1;
        d_stats.d_entriesCount -= 1;
        d_stats.d_expiredItems += 1;
        d_stats.d_memoryUsed -= sizeof(*mapIt);
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
        d_stats.d_cacheMisses += 1;
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

    if (result) {
      d_stats.d_cacheHits += 1;
    }
    else {
      d_stats.d_cacheMisses += 1;
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
        shard.d_entriesCount -= 1;
        d_stats.d_entriesCount -= 1;
        d_stats.d_expiredItems += 1;
        d_stats.d_memoryUsed -= sizeof(*mapIt);
        map->erase(mapIt);
      }
    }

    return result;
  }

  bool remove(const K& key) override
  {
    size_t hash = Hash{}(key);
    size_t shardIndex = hash % d_settings.d_shardCount;
    auto& shard = d_shards.at(shardIndex);
    auto map = shard.d_map.write_lock();

    auto mapIt = map->find(key);
    if (mapIt == map->end()) {
      return false;
    }

    shard.d_entriesCount -= 1;
    d_stats.d_entriesCount -= 1;
    d_stats.d_memoryUsed -= sizeof(*mapIt);
    map->erase(mapIt);
    return true;
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
            d_stats.d_memoryUsed -= sizeof(*it);
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

      d_stats.d_entriesCount -= removed;
      d_stats.d_expiredItems += removed;

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
        // TODO update memory usage
        removed += toRemove;
      }
      else {
        removed += map->size();
        map->clear();
        shard.d_entriesCount = 0;
      }
    }

    d_stats.d_entriesCount -= removed;
    d_stats.d_kickedItems += removed;

    return removed;
  }

  [[nodiscard]] virtual const typename GenericCacheInterface<K, V>::Stats& getStats() const override
  {
    return d_stats;
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
  typename GenericCacheInterface<K, V>::Stats d_stats{"filter=\"none\""};
};

class BloomFilter : public GenericCacheInterface<std::string, std::optional<std::string>>
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
    d_stats.d_memoryUsed += sizeof(*this);
  }

  virtual ~BloomFilter() {};

  void insertKey(const std::string& key) override
  {
    d_sbf.lock()->add(key);
    d_stats.d_entriesCount += 1;
  }

  void insert(const std::string& key, [[maybe_unused]] std::optional<std::string> value) override
  {
    insertKey(key);
  }

  bool contains(const std::string& key) override
  {
    auto result = d_sbf.lock()->test(key);
    if (result) {
      d_stats.d_cacheHits += 1;
    }
    else {
      d_stats.d_cacheMisses += 1;
    }
    return result;
  }

  bool remove([[maybe_unused]] const std::string& key) override
  {
    // Unsupported
    return false;
  }

  bool getValue(const std::string& key, [[maybe_unused]] std::optional<std::string>& value) override
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

  [[nodiscard]] virtual const GenericCacheInterface<std::string, std::optional<std::string>>::Stats& getStats() const override
  {
    return d_stats;
  }

private:
  BloomSettings d_settings;
  LockGuarded<bf::stableBF> d_sbf;
  GenericCacheInterface<std::string, std::optional<std::string>>::Stats d_stats{"filter=\"bloom\""};
};

class CuckooFilter : public GenericCacheInterface<std::string, std::optional<std::string>>
{
public:
  struct CuckooSettings
  {
    unsigned int d_maxKicks{500};
    uint32_t d_maxEntries{100000};
    uint32_t d_bucketSize{4};
    uint32_t d_fingerprintBits{8};
    bool d_ttlEnabled;
    uint32_t d_ttl;
    uint32_t d_ttlBits{32};
    uint32_t d_ttlResolution{1};
    bool d_lruEnabled;

    // derived fields
    size_t d_fingerprintBytes{0};
    size_t d_ttlBytes{0};
    size_t d_dataBlockSize{0};
  };

  // TODO: Change TTL approach to do periodical scans (or updates in purgeExpired)
  CuckooFilter(CuckooSettings settings) :
    d_settings(settings), d_numBuckets(getBucketCount(settings.d_maxEntries, settings.d_bucketSize)), d_numBucketsMask(d_numBuckets - 1), d_buckets(d_numBuckets)
  {
    if (d_settings.d_fingerprintBits > 32) {
      // TODO: Return an error?
      d_settings.d_fingerprintBits = 32;
    }

    if (d_settings.d_ttlBits > 32) {
      // TODO: Return an error?
      d_settings.d_ttlBits = 32;
    }

    d_fingerprintMask = (1L << d_settings.d_fingerprintBits) - 1;
    d_settings.d_fingerprintBytes = (d_settings.d_fingerprintBits + 7) / 8;
    d_settings.d_ttlBytes = (d_settings.d_ttlBits + 7) / 8;
    d_settings.d_dataBlockSize = d_settings.d_fingerprintBytes;
    if (d_settings.d_lruEnabled) {
      d_settings.d_dataBlockSize += 1;
    }
    if (d_settings.d_ttlEnabled) {
      d_settings.d_dataBlockSize += d_settings.d_ttlBytes;
    }

    timespec now;
    gettime(&now);
    d_lastScan = now.tv_sec;
    for (size_t i = 0; i < d_numBuckets; ++i) {
      *d_buckets[i].lock() = Bucket(d_settings);
    }

    d_stats.d_memoryUsed += sizeof(*this) + std::transform_reduce(d_buckets.begin(), d_buckets.end(), 0, std::plus<>(), [](LockGuarded<Bucket>& bucket) { return sizeof(Bucket) + bucket.lock()->d_data.size(); });
  }

  virtual ~CuckooFilter() {};

  void insertKey(const std::string& key) override
  {
    auto [i1, fp] = getIndexAndFingerprint(key);
    auto [data, block] = newDataBlock(fp);

    auto inserted = d_buckets[i1].lock()->insert(block, d_settings, d_stats);

    if (inserted) {
      return;
    }

    auto i2 = altIndex(i1, fp);
    inserted = d_buckets[i2].lock()->insert(block, d_settings, d_stats);

    if (inserted) {
      return;
    }

    // Cuckoo eviction
    size_t cur_index = i1;
    for (size_t kick = 0; kick < d_settings.d_maxKicks; ++kick) {
      if (d_settings.d_lruEnabled) {
        bool kicked = d_buckets[cur_index].lock()->kickLru(block, d_settings);
        if (!kicked) {
          return;
        }
      }
      else {
        d_buckets[cur_index].lock()->kickRandom(block, d_settings, d_gen);
      }
      cur_index = altIndex(cur_index, block.getFingerprint(d_settings));

      if (d_buckets[cur_index].lock()->insert(block, d_settings, d_stats)) {
        return;
      }
    }

    d_stats.d_kickedItems += 1;
    return; // Filter is full
  }

  void insert(const std::string& key, [[maybe_unused]] std::optional<std::string> value) override
  {
    insertKey(key);
  }

  bool contains(const std::string& key) override
  {
    auto [i1, fp] = getIndexAndFingerprint(key);
    timespec now;
    gettime(&now);

    auto result = d_buckets[i1].lock()->contains(fp, d_settings);
    if (result) {
      d_stats.d_cacheHits += 1;
      return true;
    }

    auto i2 = altIndex(i1, fp);
    result = d_buckets[i2].lock()->contains(fp, d_settings);

    if (result) {
      d_stats.d_cacheHits += 1;
    }
    else {
      d_stats.d_cacheMisses += 1;
    }

    return result;
  }

  bool remove(const std::string& key) override
  {
    auto [i1, fp] = getIndexAndFingerprint(key);

    auto removed = d_buckets[i1].lock()->remove(fp, d_settings);
    if (removed) {
      d_stats.d_entriesCount -= 1;
      return true;
    }

    auto i2 = altIndex(i1, fp);
    removed = d_buckets[i2].lock()->remove(fp, d_settings);

    if (removed) {
      d_stats.d_entriesCount -= 1;
    }
    return removed;
  }

  bool getValue(const std::string& key, [[maybe_unused]] std::optional<std::string>& value) override
  {
    return contains(key);
  }

  size_t purgeExpired([[maybe_unused]] size_t upTo, time_t now) override
  {
    if (!d_settings.d_ttlEnabled) {
      return 0;
    }

    size_t cycles = (now - d_lastScan) / d_settings.d_ttlResolution;
    size_t adjustment = (now - d_lastScan) % d_settings.d_ttlResolution;
    size_t removed = 0;
    for (auto& bucket : d_buckets) {
      auto lock = bucket.lock();
      if (d_settings.d_ttlEnabled) {
        removed += lock->ageAndRemoveExpired(d_settings, cycles);
      }
    }
    d_lastScan = now - adjustment;
    d_stats.d_entriesCount -= removed;
    d_stats.d_expiredItems += removed;
    return removed;
  }

  size_t expunge([[maybe_unused]] size_t upTo = 0) override
  {
    // TODO: what to do here?
    return 0;
  }

  [[nodiscard]] virtual const GenericCacheInterface<std::string, std::optional<std::string>>::Stats& getStats() const override
  {
    return d_stats;
  }

private:
  using Fingerprint = uint32_t;
  using stats_t = GenericCacheInterface<std::string, std::optional<std::string>>::Stats;

  static constexpr Fingerprint EMPTY_FINGERPRINT = 0;

  static size_t getBucketCount(size_t maxEntries, size_t bucketSize)
  {
    size_t bucketCount = (maxEntries + bucketSize - 1) / bucketSize;

    size_t numBuckets = 1;
    while (numBuckets < bucketCount) {
      numBuckets <<= 1;
    }
    return numBuckets;
  }

  struct DataBlock
  {
    unsigned char* d_dataStart;
    unsigned char* d_dataEnd;

    explicit DataBlock(unsigned char* start, unsigned char* end) :
      d_dataStart(start), d_dataEnd(end) {}

    size_t fingerprintStart([[maybe_unused]] const CuckooSettings& settings) const
    {
      return 0;
    }

    size_t lruCounterStart(const CuckooSettings& settings) const
    {
      return settings.d_fingerprintBytes;
    }

    size_t ttlStart(const CuckooSettings& settings) const
    {
      return settings.d_fingerprintBytes + (settings.d_lruEnabled ? 1 : 0);
    }

    void clear()
    {
      std::fill(this->d_dataStart, this->d_dataEnd, 0);
    }

    void swapWith(DataBlock& other)
    {
      std::swap_ranges(this->d_dataStart, this->d_dataEnd, other.d_dataStart);
    }

    Fingerprint getFingerprint(const CuckooSettings& settings) const
    {
      Fingerprint fp = 0;
      memcpy(&fp, d_dataStart, settings.d_fingerprintBytes);
      return fp;
    }

    uint32_t getTtl(const CuckooSettings& settings) const
    {
      uint32_t ttl = 0;
      memcpy(&ttl, d_dataStart + ttlStart(settings), settings.d_ttlBytes);
      return ttl;
    }

    uint8_t getLru(const CuckooSettings& settings) const
    {
      return *(d_dataStart + lruCounterStart(settings));
    }
  };

  // TODO: handle stats updates inside bucket operations (expired, etc.)
  struct Bucket
  {
    std::vector<unsigned char> d_data;

    Bucket() {}

    Bucket(const CuckooSettings& settings) :
      d_data(settings.d_bucketSize * settings.d_dataBlockSize, 0)
    {}

    bool insert(DataBlock& block, const CuckooSettings& settings, stats_t& stats)
    {
      Fingerprint fp = block.getFingerprint(settings);
      for (size_t i = 0; i < settings.d_bucketSize; ++i) {
        DataBlock slot = getSlot(i, settings);
        Fingerprint storedFingerprint = slot.getFingerprint(settings);
        bool reinsert = storedFingerprint == fp;
        if (reinsert || storedFingerprint == EMPTY_FINGERPRINT) {
          if (!reinsert) {
            slot.swapWith(block);
            stats.d_entriesCount += 1;
          }
          else {
            // TODO: Merge associated data
            if (settings.d_lruEnabled) {
              accessSlot(i, settings);
            }
          }

          return true;
        }
      }
      return false;
    }

    bool contains(Fingerprint fp, const CuckooSettings& settings)
    {
      for (size_t i = 0; i < settings.d_bucketSize; ++i) {
        Fingerprint storedFingerprint = 0;
        // TODO: move this over to data block, since it should support values not aligned to bytes
        memcpy(&storedFingerprint, &d_data[fingerprintStart(i, settings)], settings.d_fingerprintBytes);
        if (storedFingerprint == fp) {
          if (settings.d_lruEnabled) {
            accessSlot(i, settings);
          }
          return true;
        }
      }
      return false;
    }

    bool remove(Fingerprint fp, const CuckooSettings& settings)
    {
      for (size_t i = 0; i < settings.d_bucketSize; ++i) {
        DataBlock slot = getSlot(i, settings);
        Fingerprint storedFingerprint = slot.getFingerprint(settings);
        if (storedFingerprint == fp) {
          slot.clear();
          return true;
        }
      }
      return false;
    }

    // For cuckoo eviction - returns evicted fingerprint
    void kickRandom(DataBlock& data_block, const CuckooSettings& settings, std::mt19937& rng)
    {
      size_t pos = rng() % settings.d_bucketSize;
      DataBlock(d_data.data() + fingerprintStart(pos, settings), d_data.data() + fingerprintStart(pos, settings) + settings.d_fingerprintBytes).swapWith(data_block);
    }

    // For LRU cuckoo eviction - returns true if fingeprint was evicted - the fingeprint is stored in kicked_fp
    bool kickLru(DataBlock& data_block, const CuckooSettings& settings)
    {
      uint8_t min = data_block.getLru(settings);
      if (min == 0) {
        min = std::numeric_limits<uint8_t>::max();
      }
      size_t pos = settings.d_bucketSize;
      for (size_t i = 0; i < settings.d_bucketSize; ++i) {
        DataBlock slot = getSlot(i, settings);
        uint8_t thisCounter = slot.getLru(settings);
        if (thisCounter < min) {
          pos = i;
          min = thisCounter;
        }
      }
      if (pos < settings.d_bucketSize) {
        DataBlock slot = getSlot(pos, settings);
        slot.swapWith(data_block);
        return true;
      }
      else {
        return false;
      }
    }

    size_t ageAndRemoveExpired(const CuckooSettings& settings, size_t cycles)
    {
      size_t removed = 0;
      for (size_t i = 0; i < settings.d_bucketSize; ++i) {
        DataBlock slot = getSlot(i, settings);
        uint32_t ttl = slot.getTtl(settings);
        if (ttl <= cycles) {
          removed += 1;
          slot.clear();
        }
        else {
          ttl -= cycles;
        }
      }
      return removed;
    }

    DataBlock getSlot(int slot, const CuckooSettings& settings)
    {
      return DataBlock(d_data.data() + slot * settings.d_dataBlockSize, d_data.data() + (slot + 1) * settings.d_dataBlockSize);
    }

    // TODO: maybe remove this if we age all LRU counters on some generic scan
    void accessSlot(int slot, const CuckooSettings& settings)
    {
      if ((uint8_t)d_data[lruCounterStart(slot, settings)] == 255) {
        // Age all counters when one saturates
        for (uint32_t i = 0; i < settings.d_bucketSize; i++) {
          // TODO: check does this ruin eviction process - an item that is used more often might be perceived as less used because of this, if it was inside a very active bucket
          d_data[lruCounterStart(i, settings)] >>= 1;
        }
      }
      d_data[lruCounterStart(slot, settings)] += 1;
    }

    size_t fingerprintStart(size_t index, const CuckooSettings& settings)
    {
      return index * settings.d_dataBlockSize;
    }

    size_t lruCounterStart(size_t index, const CuckooSettings& settings)
    {
      return index * settings.d_dataBlockSize + settings.d_fingerprintBytes;
    }
  };

  std::tuple<std::vector<unsigned char>, DataBlock> newDataBlock(Fingerprint fp) const
  {
    std::vector<unsigned char> data(d_settings.d_dataBlockSize, 0);
    DataBlock block(data.data(), data.data() + data.size());
    memcpy(&data[block.fingerprintStart(d_settings)], &fp, d_settings.d_fingerprintBytes);
    if (d_settings.d_ttlEnabled) {
      memcpy(&data[block.ttlStart(d_settings)], &d_settings.d_ttl, d_settings.d_ttlBytes);
    }
    if (d_settings.d_lruEnabled) {
      data[block.getLru(d_settings)] = 1;
    }
    // Explicitly move - block holds a pointer to data - without move this gets copied and the reference becomes invalid
    return {std::move(data), std::move(block)};
  }

  std::tuple<size_t, Fingerprint> getIndexAndFingerprint(const std::string& data) const
  {
    uint32_t fingerprint_raw = murmurHash(data);

    Fingerprint fp = static_cast<Fingerprint>((fingerprint_raw & d_fingerprintMask));
    if (fp == EMPTY_FINGERPRINT)
      fp = 1; // Avoid empty fingerprint

    size_t i1 = fingerprint_raw & d_numBucketsMask;

    return {i1, fp};
  }

  uint32_t murmurHash(const std::string& data, const uint32_t seed = 0x9747b28c) const
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

  size_t altIndex(size_t index, Fingerprint fp) const
  {
    uint32_t bucket = murmurHash(std::to_string(fp));
    return (index ^ (bucket & d_numBucketsMask)) & d_numBucketsMask;
  }

  CuckooSettings d_settings;
  size_t d_numBuckets;
  size_t d_numBucketsMask;
  Fingerprint d_fingerprintMask;
  std::vector<LockGuarded<Bucket>> d_buckets;
  std::mt19937 d_gen;
  time_t d_lastScan;
  GenericCacheInterface<std::string, std::optional<std::string>>::Stats d_stats{"filter=\"cuckoo\""};
};
