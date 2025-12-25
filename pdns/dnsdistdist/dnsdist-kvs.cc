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

#include "dnsdist-kvs.hh"
#include "dnsdist-lua-types.hh"
#include "dolog.hh"

#include <memory>
#include <sys/stat.h>
#include <boost/variant.hpp>

std::vector<std::string> KeyValueLookupKeySourceIP::getKeys(const ComboAddress& addr)
{
  std::vector<std::string> result;
  ComboAddress truncated(addr);

  std::string key;
  if (truncated.isIPv4()) {
    truncated.truncate(d_v4Mask);
    key.reserve(sizeof(truncated.sin4.sin_addr.s_addr) + (d_includePort ? sizeof(truncated.sin4.sin_port) : 0));
    key.append(reinterpret_cast<const char*>(&truncated.sin4.sin_addr.s_addr), sizeof(truncated.sin4.sin_addr.s_addr));
  }
  else if (truncated.isIPv6()) {
    truncated.truncate(d_v6Mask);
    key.reserve(sizeof(truncated.sin6.sin6_addr.s6_addr) + (d_includePort ? sizeof(truncated.sin4.sin_port) : 0));
    key.append(reinterpret_cast<const char*>(&truncated.sin6.sin6_addr.s6_addr), sizeof(truncated.sin6.sin6_addr.s6_addr));
  }

  if (d_includePort) {
    key.append(reinterpret_cast<const char*>(&truncated.sin4.sin_port), sizeof(truncated.sin4.sin_port));
  }

  result.push_back(std::move(key));

  return result;
}

std::vector<std::string> KeyValueLookupKeySuffix::getKeys(const DNSName& qname)
{
  if (qname.empty() || qname.isRoot()) {
    return {};
  }

  auto lowerQName = qname.makeLowerCase();
  size_t labelsCount = lowerQName.countLabels();
  if (d_minLabels != 0) {
    if (labelsCount < d_minLabels) {
      return {};
    }
    labelsCount -= (d_minLabels - 1);
  }

  std::vector<std::string> result;
  result.reserve(labelsCount);

  while(!lowerQName.isRoot()) {
    result.emplace_back(d_wireFormat ? lowerQName.toDNSString() : lowerQName.toStringRootDot());
    labelsCount--;
    if (!lowerQName.chopOff() || labelsCount == 0) {
      break;
    }
  }

  return result;
}

#ifdef HAVE_LMDB

bool LMDBKVStore::getValue(const std::string& key, std::string& value)
{
  try {
    auto transaction = d_env->getROTransaction();
    MDBOutVal result;
    int rc = transaction->get(d_dbi, MDBInVal(key), result);
    if (rc == 0) {
      value = result.get<std::string>();
      return true;
    }
    else if (rc == MDB_NOTFOUND) {
      return false;
    }
  }
  catch (const std::exception& e) {
    vinfolog("Error while looking up key '%s' from LMDB file '%s', database '%s': %s", key, d_fname, d_dbName, e.what());
  }
  return false;
}

bool LMDBKVStore::keyExists(const std::string& key)
{
  try {
    auto transaction = d_env->getROTransaction();
    MDBOutVal result;
    int rc = transaction->get(d_dbi, MDBInVal(key), result);
    if (rc == 0) {
      return true;
    }
    else if (rc == MDB_NOTFOUND) {
      return false;
    }
  }
  catch (const std::exception& e) {
    vinfolog("Error while looking up key '%s' from LMDB file '%s', database '%s': %s", key, d_fname, d_dbName, e.what());
  }
  return false;
}

bool LMDBKVStore::getRangeValue(const std::string& key, std::string& value)
{
  try {
    auto transaction = d_env->getROTransaction();
    auto cursor = transaction->getROCursor(d_dbi);
    MDBOutVal actualKey;
    MDBOutVal result;
    // for range-based lookups, we expect the data in LMDB
    // to be stored with the last value of the range as key
    // and the first value of the range as data, sometimes
    // followed by any other content we don't care about
    // range-based lookups are mostly useful for network ranges,
    // for which we expect addresses to be stored in network byte
    // order

    // retrieve the first key greater or equal to our key
    int rc = cursor.lower_bound(MDBInVal(key), actualKey, result);

    if (rc == 0) {
      auto last = actualKey.get<std::string>();
      if (last.size() != key.size() || key > last) {
        return false;
      }

      value = result.get<std::string>();
      if (value.size() < key.size()) {
        return false;
      }

      // take the first part of the data, which should be
      // the first address of the range
      auto first = value.substr(0, key.size());
      if (first.size() != key.size() || key < first) {
        return false;
      }

      return true;
    }
    else if (rc == MDB_NOTFOUND) {
      return false;
    }
  }
  catch (const std::exception& e) {
    vinfolog("Error while looking up a range from LMDB file '%s', database '%s': %s", d_fname, d_dbName, e.what());
  }
  return false;
}

#endif /* HAVE_LMDB */

#ifdef HAVE_CDB

CDBKVStore::CDBKVStore(const std::string& fname, time_t refreshDelay): d_fname(fname), d_refreshDelay(refreshDelay)
{
  d_refreshing.clear();

  time_t now = time(nullptr);
  if (d_refreshDelay > 0) {
    d_nextCheck = now + d_refreshDelay;
  }

  refreshDBIfNeeded(now);
}

CDBKVStore::~CDBKVStore() {
}

bool CDBKVStore::reload(const struct stat& st)
{
  auto newCDB = std::make_unique<CDB>(d_fname);
  {
    *(d_cdb.write_lock()) = std::move(newCDB);
  }
  d_mtime = st.st_mtime;
  return true;
}

bool CDBKVStore::reload()
{
  struct stat st;
  if (stat(d_fname.c_str(), &st) == 0) {
    return reload(st);
  }
  else {
    warnlog("Error while retrieving the last modification time of CDB database '%s': %s", d_fname, stringerror());
    return false;
  }
}

void CDBKVStore::refreshDBIfNeeded(time_t now)
{
  if (d_refreshing.test_and_set()) {
    /* someone else is already refreshing */
    return;
  }

  try {
    struct stat st;
    if (stat(d_fname.c_str(), &st) == 0) {
      if (st.st_mtime > d_mtime) {
        reload(st);
      }
    }
    else {
      warnlog("Error while retrieving the last modification time of CDB database '%s': %s", d_fname, stringerror());
    }
    d_nextCheck = now + d_refreshDelay;
    d_refreshing.clear();
  }
  catch (...) {
    d_refreshing.clear();
    throw;
  }
}

bool CDBKVStore::getValue(const std::string& key, std::string& value)
{
  time_t now = time(nullptr);

  try {
    if (d_nextCheck != 0 && now >= d_nextCheck) {
      refreshDBIfNeeded(now);
    }

    {
      auto cdb = d_cdb.read_lock();
      if (*cdb && (*cdb)->findOne(key, value)) {
        return true;
      }
    }
  }
  catch (const std::exception& e) {
    vinfolog("Error while looking up key '%s' from CDB file '%s': %s", key, d_fname, e.what());
  }
  return false;
}

bool CDBKVStore::keyExists(const std::string& key)
{
  time_t now = time(nullptr);

  try {
    if (d_nextCheck != 0 && now >= d_nextCheck) {
      refreshDBIfNeeded(now);
    }

    {
      auto cdb = d_cdb.read_lock();
      if (!*cdb) {
        return false;
      }

      return (*cdb)->keyExists(key);
    }
  }
  catch (const std::exception& e) {
    vinfolog("Error while looking up key '%s' from CDB file '%s': %s", key, d_fname, e.what());
  }
  return false;
}

#endif /* HAVE_CDB */

#ifdef HAVE_MMDB

#include "ext/json11/json11.hpp"

bool MMDBKVStore::keyExists(const std::string& key)
{
  auto addr = ComboAddress(key);
  return d_mmdb->exists(addr);
}
bool MMDBKVStore::getValue(const std::string& key, std::string& value)
{
  auto addr = ComboAddress(key);
  LuaAny ret;
  bool result = d_mmdb->query(ret, d_queryParams, addr);
  if (ret.type() == typeid(std::string)) {
    value = boost::get<std::string>(ret);
    return result;
  }

  json11::Json json = this->parseAny(ret);
  json.dump(value);

  return result;
}

json11::Json MMDBKVStore::parseAny(const LuaAny& any)
{
  if (any.type() == typeid(std::string)) {
    return json11::Json(boost::get<std::string>(any));
  }
  else if (any.type() == typeid(int)) {
    return json11::Json(boost::get<int>(any));
  }
  else if (any.type() == typeid(double)) {
    return json11::Json(boost::get<double>(any));
  }
  else if (any.type() == typeid(bool)) {
    return json11::Json(boost::get<bool>(any));
  }
  else if (any.type() == typeid(LuaArray<LuaAny>)) {
    auto luaArray = boost::get<LuaArray<LuaAny>>(any);
    std::vector<json11::Json> array(luaArray.size());
    for (auto& kv : luaArray) {
      array.emplace_back(parseAny(kv.second));
    }
    return json11::Json(array);
  }
  else if (any.type() == typeid(LuaAssociativeTable<LuaAny>)) {
    auto luaTable = boost::get<LuaAssociativeTable<LuaAny>>(any);
    std::unordered_map<std::string, json11::Json> map(luaTable.size());
    for (auto& kv : map) {
      map.emplace(kv.first, kv.second);
    }
    return json11::Json(map);
  }
  else {
    return json11::Json();
  }
}

#endif // HAVE_MMDB

#ifdef HAVE_REDIS

RedisKVStore::RedisKVStore(const std::shared_ptr<RedisClient>& redisClient, std::optional<std::string> lookupAction, std::optional<std::string> dataName, bool copyCacheEnabled, unsigned int copyCacheTtl, std::shared_ptr<GenericCacheInterface<std::string, std::string>> resultCache, std::shared_ptr<GenericFilterInterface<std::string>> negativeCache, std::shared_ptr<GenericFilterInterface<std::string>> copyCacheFilter, std::shared_ptr<RedisStats> stats) :
  d_stats(stats)
{
  std::unique_ptr<RedisLookupAction> command;
  if (lookupAction && !boost::iequals(lookupAction.value(), "get")) {
    if (!dataName) {
      throw std::runtime_error("Option 'dataName' is required for lookup action " + lookupAction.value());
    }
    if (boost::iequals(lookupAction.value(), "hget")) {
      command = std::make_unique<RedisHGetLookupAction>(dataName.value());
    }
    else if (boost::iequals(lookupAction.value(), "sismember")) {
      command = std::make_unique<RedisSismemberLookupAction>(dataName.value());
    }
    else if (boost::iequals(lookupAction.value(), "sscan")) {
      command = std::make_unique<RedisSscanLookupAction>(dataName.value());
    }
    else {
      throw std::runtime_error("Unknown lookup action: " + lookupAction.value());
    }
  }
  else {
    if (dataName) {
      command = std::make_unique<RedisGetLookupAction>(dataName.value());
    }
    else {
      command = std::make_unique<RedisGetLookupAction>();
    }
  }
  d_redis = std::make_unique<RedisKVClient>(redisClient, std::move(command), stats);
  if (negativeCache) {
    d_redis = std::make_unique<NegativeCachingRedisClient>(std::move(d_redis), negativeCache);
  }
  if (resultCache) {
    d_redis = std::make_unique<ResultCachingRedisClient>(std::move(d_redis), resultCache);
  }
  if (copyCacheEnabled) {
    if (copyCacheFilter) {
      d_redis = std::make_unique<FilteringCopyCachingRedisClient>(std::move(d_redis), copyCacheTtl, copyCacheFilter);
    }
    else {
      d_redis = std::make_unique<CopyCachingRedisClient>(std::move(d_redis), copyCacheTtl);
    }
  }
}

bool RedisKVStore::reload()
{
  return true;
}

bool RedisKVStore::getValue(const std::string& key, std::string& value)
{
  auto result = d_redis->getValue(key, value);
  if (result) {
    d_stats->d_successfulLookups += 1;
  }
  else {
    d_stats->d_failedLookups += 1;
  }
  return result;
}

bool RedisKVStore::keyExists(const std::string& key)
{
  auto result = d_redis->keyExists(key);
  if (result) {
    d_stats->d_successfulLookups += 1;
  }
  else {
    d_stats->d_failedLookups += 1;
  }
  return result;
}

#endif // HAVE_REDIS
