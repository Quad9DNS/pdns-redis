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

#include "dnsdist-lua-types.hh"
#include "dnsdist-lua.hh"
#include "generic-cache.hh"
#include "channel.hh"
#include "lock.hh"
#include <thread>
#include <yahttp/yahttp.hpp>
#include <hiredis/hiredis.h>
#include <memory>
#include <string>
#include "redis-stats.hh"

class RedisClient;

template <typename T>
class RedisReplyInterface
{
public:
  virtual ~RedisReplyInterface() {};
  virtual bool ok() const = 0;
  virtual T getValue() const = 0;
  virtual std::string getError() const = 0;
};

template <typename T>
class RedisReply : public RedisReplyInterface<T>
{
public:
  RedisReply(redisReply* reply) :
    d_reply(reply)
  {
  }
  virtual ~RedisReply() override
  {
    if (d_reply) {
      freeReplyObject(d_reply);
    }
  }

  virtual bool ok() const override
  {
    return d_reply;
  }

  virtual std::string getError() const override
  {
    if (d_reply) {
      return std::string(d_reply->str, d_reply->len);
    }
    else {
      return std::string();
    }
  }

protected:
  redisReply* d_reply;
};

template <typename S, typename T>
class MappedRedisReply : public RedisReplyInterface<T>
{
public:
  MappedRedisReply(std::unique_ptr<RedisReplyInterface<S>> inner) :
    d_inner(std::move(inner)) {};

  virtual bool ok() const override
  {
    return d_inner->ok();
  }

  virtual std::string getError() const override
  {
    return d_inner->getError();
  }

  virtual T getValue() const override = 0;

protected:
  std::unique_ptr<RedisReplyInterface<S>> d_inner;
};

class RedisStringReply : public RedisReply<std::string>
{
public:
  RedisStringReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply && (d_reply->str);
  }
  std::string getValue() const override
  {
    return std::string(d_reply->str, d_reply->len);
  }
};

class RedisIntReply : public RedisReply<long long>
{
public:
  RedisIntReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply && d_reply->type == REDIS_REPLY_INTEGER;
  }
  long long getValue() const override
  {
    return d_reply->integer;
  }
};

class RedisIntAsStringReply : public MappedRedisReply<long long, std::string>
{
public:
  RedisIntAsStringReply(std::unique_ptr<RedisReplyInterface<long long>> inner) :
    MappedRedisReply(std::move(inner))
  {
  }
  std::string getValue() const override
  {
    return std::to_string(d_inner->getValue());
  }
};

class RedisIntAsBoolReply : public MappedRedisReply<long long, bool>
{
public:
  RedisIntAsBoolReply(std::unique_ptr<RedisReplyInterface<long long>> inner) :
    MappedRedisReply(std::move(inner))
  {
  }
  bool getValue() const override
  {
    return d_inner->getValue() > 0;
  }
};

class RedisBoolAsStringReply : public MappedRedisReply<bool, std::string>
{
public:
  RedisBoolAsStringReply(std::unique_ptr<RedisReplyInterface<bool>> inner) :
    MappedRedisReply(std::move(inner))
  {
  }
  std::string getValue() const override
  {
    return d_inner->getValue() ? "1" : "0";
  }
};

class RedisScanAsStringReply : public RedisReply<std::string>
{
public:
  RedisScanAsStringReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY && d_reply->elements == 2;
  }
  std::string getValue() const override
  {
    auto members = d_reply->element[1];
    std::string result;
    for (size_t i = 0; i < members->elements; i++) {
      if (i) {
        result += ", ";
      }
      result += members->element[i]->str;
    }
    return result;
  }
};

class RedisScanAsBoolReply : public RedisReply<bool>
{
public:
  RedisScanAsBoolReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
#if HIREDIS_MAJOR > 0
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY && d_reply->elements == 2 && d_reply->element[0]->type == REDIS_REPLY_BIGNUM;
#else
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY && d_reply->elements == 2;
#endif
  }
  bool getValue() const override
  {
    return std::strcmp(d_reply->element[0]->str, "0") != 0;
  }
};

class RedisHashReply : public RedisReply<std::unordered_map<std::string, std::string>>
{
public:
  RedisHashReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY && d_reply->elements % 2 == 0;
  }
  std::unordered_map<std::string, std::string> getValue() const override
  {
    std::unordered_map<std::string, std::string> result{d_reply->elements / 2};
    for (size_t i = 0; i < d_reply->elements; i += 2) {
      auto key = std::string(d_reply->element[i]->str, d_reply->element[i]->len);
      auto value = std::string(d_reply->element[i + 1]->str, d_reply->element[i + 1]->len);
      result.emplace(key, value);
    }
    return result;
  }
};

class RedisArrayReply : public RedisReply<std::vector<std::pair<int, std::optional<std::string>>>>
{
public:
  RedisArrayReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY;
  }
  std::vector<std::pair<int, std::optional<std::string>>> getValue() const override
  {
    std::vector<std::pair<int, std::optional<std::string>>> result{d_reply->elements};
    for (size_t i = 0; i < d_reply->elements; i++) {
      if (d_reply->element[i]->type == REDIS_REPLY_NIL) {
        result.emplace_back(i + 1, std::nullopt);
      }
      else {
        // Lua arrays start at 1 instead of 0
        result.emplace_back(i + 1, d_reply->element[i]->str);
      }
    }
    return result;
  }
};

class RedisSetReply : public RedisReply<std::unordered_set<std::string>>
{
public:
  RedisSetReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY;
  }
  std::unordered_set<std::string> getValue() const override
  {
    std::unordered_set<std::string> result{d_reply->elements};
    for (size_t i = 0; i < d_reply->elements; i++) {
      result.emplace(d_reply->element[i]->str);
    }
    return result;
  }
};

class RedisRawReply : public RedisReply<LuaAny>
{
public:
  RedisRawReply(redisReply* reply) :
    RedisReply(reply)
  {
  }
  bool ok() const override
  {
    return d_reply;
  }
  LuaAny getValue() const override
  {
    return parseReply(d_reply).value();
  }

private:
  std::optional<LuaAny> parseReply(redisReply* reply) const
  {
    switch (reply->type) {
    case REDIS_REPLY_INTEGER:
      // TODO: narrowing conversion
      return (int)reply->integer;
      break;
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_STATUS:
#if HIREDIS_MAJOR > 0
    case REDIS_REPLY_BIGNUM:
    case REDIS_REPLY_VERB:
#endif
      return std::string(reply->str, reply->len);
      break;
    case REDIS_REPLY_ARRAY:
#if HIREDIS_MAJOR > 0
    case REDIS_REPLY_SET:
#endif
      return parseArray(reply);
      break;
#if HIREDIS_MAJOR > 0
    case REDIS_REPLY_DOUBLE:
      return reply->dval;
      break;
    case REDIS_REPLY_BOOL:
      return reply->integer > 0;
      break;
    case REDIS_REPLY_MAP:
      return parseMap(reply);
      break;
    }
#endif
    return std::nullopt;
  }

  LuaArray<LuaAny> parseArray(redisReply* data) const
  {
    LuaArray<LuaAny> res{data->elements};
    for (size_t i = 0; i < data->elements; i++) {
      if (auto value = parseReply(data->element[i])) {
        res[i] = std::make_pair(i + 1, value.value());
      }
    }
    return res;
  }

#if HIREDIS_MAJOR > 0
  LuaAssociativeTable<LuaAny> parseMap(redisReply* data) const
  {
    LuaAssociativeTable<LuaAny> res{data->elements / 2};
    for (size_t i = 0; i < data->elements / 2; i += 2) {
      if (auto value = parseReply(data->element[i + 1])) {
        res.emplace(data->element[i]->str, value.value());
      }
    }
    return res;
  }
#endif
};

template <typename T, typename... Args>
struct RedisCommand
{
  virtual std::unique_ptr<RedisReplyInterface<T>> operator()(const RedisClient& client, const Args&... args) const = 0;
};

struct RedisGetCommand : public RedisCommand<std::string, std::string>
{
  std::unique_ptr<RedisReplyInterface<std::string>> operator()(const RedisClient& client, const std::string& key) const override;
};

struct RedisExistsCommand : public RedisCommand<bool, std::string>
{
  std::unique_ptr<RedisReplyInterface<bool>> operator()(const RedisClient& client, const std::string& key) const override;
};

struct RedisHGetCommand : public RedisCommand<std::string, std::string, std::string>
{
  std::unique_ptr<RedisReplyInterface<std::string>> operator()(const RedisClient& client, const std::string& hash_key, const std::string& key) const override;
};

// This works with vector of pairs to better work with Lua interface
struct RedisHMGetCommand : public RedisCommand<std::vector<std::pair<int, std::optional<std::string>>>, std::string, std::vector<std::pair<int, std::string>>>
{
  std::unique_ptr<RedisReplyInterface<std::vector<std::pair<int, std::optional<std::string>>>>> operator()(const RedisClient& client, const std::string& hash_key, const std::vector<std::pair<int, std::string>>& fields) const override;
};

struct RedisHGetAllCommand : public RedisCommand<std::unordered_map<std::string, std::string>, std::string>
{
  std::unique_ptr<RedisReplyInterface<std::unordered_map<std::string, std::string>>> operator()(const RedisClient& client, const std::string& hash_key) const override;
};

struct RedisHExistsCommand : public RedisCommand<bool, std::string, std::string>
{
  std::unique_ptr<RedisReplyInterface<bool>> operator()(const RedisClient& client, const std::string& hash_key, const std::string& key) const override;
};

struct RedisSIsMemberCommand : public RedisCommand<bool, std::string, std::string>
{
  std::unique_ptr<RedisReplyInterface<bool>> operator()(const RedisClient& client, const std::string& set_key, const std::string& key) const override;
};

struct RedisSMembersCommand : public RedisCommand<std::unordered_set<std::string>, std::string>
{
  std::unique_ptr<RedisReplyInterface<std::unordered_set<std::string>>> operator()(const RedisClient& client, const std::string& set_key) const override;
};

struct RedisSScanCommand : public RedisCommand<bool, std::string, size_t, std::string, size_t>
{
  std::unique_ptr<RedisReplyInterface<bool>> operator()(const RedisClient& client, const std::string& set_key, const size_t& cursor, const std::string& key, const size_t& count) const override;
};

struct RedisZrangeBylexCommand : public RedisCommand<std::unordered_set<std::string>, std::string, size_t, size_t>
{
  std::unique_ptr<RedisReplyInterface<std::unordered_set<std::string>>> operator()(const RedisClient& client, const std::string& set_key, const size_t& start, const size_t& stop) const override;
};

struct RedisRawCommand : public RedisCommand<LuaAny, LuaArray<std::string>>
{
  std::unique_ptr<RedisReplyInterface<LuaAny>> operator()(const RedisClient& client, const LuaArray<std::string>& raw_command) const override;
};

class RedisLookupAction
{
public:
  RedisLookupAction(const std::string& cache_id) :
    d_cacheId(cache_id) {};
  virtual ~RedisLookupAction() = default;

  const std::string& getCacheId() const
  {
    return d_cacheId;
  }
  virtual bool getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const = 0;
  virtual std::unique_ptr<RedisReplyInterface<std::string>> getValue(const RedisClient& client, const std::string& key) const = 0;
  // TODO: Error handling for copy cache!
  virtual std::unordered_map<std::string, std::string> generateCopyCache(const RedisClient& client) const = 0;
  virtual std::unique_ptr<RedisReplyInterface<bool>> keyExists(const RedisClient& client, const std::string& key) const = 0;

protected:
  std::string d_cacheId;
};

class RedisGetLookupAction : public RedisLookupAction
{
public:
  RedisGetLookupAction(const std::string& prefix = "") :
    RedisLookupAction("KEY_" + prefix), d_prefix(prefix)
  {
  }
  bool getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const override;
  std::unique_ptr<RedisReplyInterface<std::string>> getValue(const RedisClient& client, const std::string& key) const override;
  std::unordered_map<std::string, std::string> generateCopyCache(const RedisClient& client) const override;
  std::unique_ptr<RedisReplyInterface<bool>> keyExists(const RedisClient& client, const std::string& key) const override;

private:
  std::string d_prefix;
  RedisGetCommand d_getCommand;
  RedisExistsCommand d_existsCommand;
};

class RedisHGetLookupAction : public RedisLookupAction
{
public:
  RedisHGetLookupAction(const std::string& hash_key) :
    RedisLookupAction("HGET_" + hash_key), d_hash_key(hash_key)
  {
  }
  bool getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const override;
  std::unique_ptr<RedisReplyInterface<std::string>> getValue(const RedisClient& client, const std::string& key) const override;
  std::unordered_map<std::string, std::string> generateCopyCache(const RedisClient& client) const override;
  std::unique_ptr<RedisReplyInterface<bool>> keyExists(const RedisClient& client, const std::string& key) const override;

private:
  std::string d_hash_key;
  RedisHGetCommand d_getCommand;
  RedisHExistsCommand d_existsCommand;
  RedisHGetAllCommand d_getAllCommand;
};

class RedisSismemberLookupAction : public RedisLookupAction
{
public:
  RedisSismemberLookupAction(const std::string& set_key) :
    RedisLookupAction("SISMEMBER_" + set_key), d_set_key(set_key)
  {
  }
  bool getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const override;
  std::unique_ptr<RedisReplyInterface<std::string>> getValue(const RedisClient& client, const std::string& key) const override;
  std::unordered_map<std::string, std::string> generateCopyCache(const RedisClient& client) const override;
  std::unique_ptr<RedisReplyInterface<bool>> keyExists(const RedisClient& client, const std::string& key) const override;

private:
  std::string d_set_key;
  RedisSIsMemberCommand d_sIsMemberCommand;
  RedisSMembersCommand d_sMembersCommand;
};

class RedisSscanLookupAction : public RedisLookupAction
{
public:
  RedisSscanLookupAction(const std::string& set_key) :
    RedisLookupAction("SISMEMBER_" + set_key), d_set_key(set_key)
  {
  }
  bool getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const override;
  std::unique_ptr<RedisReplyInterface<std::string>> getValue(const RedisClient& client, const std::string& key) const override;
  std::unordered_map<std::string, std::string> generateCopyCache(const RedisClient& client) const override;
  std::unique_ptr<RedisReplyInterface<bool>> keyExists(const RedisClient& client, const std::string& key) const override;

private:
  std::string d_set_key;
  RedisSScanCommand d_sScanCommand;
};

class RedisClient
{
public:
  RedisClient(const std::string& url, bool enablePipeline = true, uint32_t pipelineInterval = 10)
  {
    if (enablePipeline) {
      d_executor = std::make_unique<PipelineExecutor>(url, pipelineInterval);
    }
    else {
      d_executor = std::make_unique<DirectExecutor>(url);
    }
  }

  redisReply* executeCommand(const char* format, ...) const;
  redisReply* executeCommandArgv(std::vector<std::string> args) const;

  const YaHTTP::URL& getUrl() const
  {
    return d_executor->getUrl();
  }

private:
  class RedisConnection
  {
  public:
    RedisConnection(const std::string& url);
    bool reconnect();
    LockGuardedHolder<const std::unique_ptr<redisContext, decltype(&redisFree)>> getConnection() const
    {
      return d_context.read_only_lock();
    }

    bool needsReconnect()
    {
      auto connection = d_context.read_only_lock();
      return connection->get() == nullptr || connection->get()->err != 0;
    }

    const YaHTTP::URL& getUrl() const
    {
      return d_url;
    }

  private:
    mutable LockGuarded<std::unique_ptr<redisContext, decltype(&redisFree)>> d_context{std::unique_ptr<redisContext, decltype(&redisFree)>(nullptr, redisFree)};
    YaHTTP::URL d_url;
  };

  class Executor
  {
  public:
    virtual ~Executor() = default;
    virtual redisReply* executeCommand(const char* format, va_list ap) const = 0;
    virtual redisReply* executeCommandArgv(int argc, const char** argv, const size_t* argvlen) const = 0;
    virtual const YaHTTP::URL& getUrl() const = 0;
  };

  class DirectExecutor : public Executor
  {
  public:
    DirectExecutor(const std::string& url) :
      d_connection(url)
    {
    }
    redisReply* executeCommand(const char* format, va_list ap) const override;
    redisReply* executeCommandArgv(int argc, const char** argv, const size_t* argvlen) const override;

    const YaHTTP::URL& getUrl() const override
    {
      return d_connection.getUrl();
    }

  private:
    RedisConnection d_connection;
  };

  class PipelineExecutor : public Executor
  {
  public:
    PipelineExecutor(const std::string& url, uint32_t pipelineInterval);
    ~PipelineExecutor();
    redisReply* executeCommand(const char* format, va_list ap) const override;
    redisReply* executeCommandArgv(int argc, const char** argv, const size_t* argvlen) const override;

    const YaHTTP::URL& getUrl() const override
    {
      return d_connection.getUrl();
    }

  private:
    void maintenanceThread();
    redisReply* pipelineCommand(const char* command, size_t len) const;

    struct PipelineCommand
    {
      typedef std::function<void(redisReply*)> callback_t;
      const char* command;
      size_t length;
      callback_t callback;
    };
    RedisConnection d_connection;
    uint32_t d_interval;
    pdns::channel::Sender<PipelineCommand> d_pipelineSender;
    pdns::channel::Receiver<PipelineCommand> d_pipelineReceiver;

    std::atomic<bool> d_exiting{false};
    std::thread d_thread;
  };

  std::unique_ptr<Executor> d_executor;
};

class RedisKVClientInterface
{
public:
  virtual ~RedisKVClientInterface() = default;
  virtual bool getValue(const std::string& key, std::string& value) = 0;
  virtual std::unordered_map<std::string, std::string> generateCopyCache() = 0;
  virtual bool keyExists(const std::string& key) = 0;
};

class ResultCachingRedisClient : public RedisKVClientInterface
{
public:
  ResultCachingRedisClient(std::unique_ptr<RedisKVClientInterface> client, std::shared_ptr<GenericCacheInterface<std::string, std::string>> cache) :
    d_client(std::move(client)), d_resultCache(cache)
  {
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::unique_ptr<RedisKVClientInterface> d_client;
  std::shared_ptr<GenericCacheInterface<std::string, std::string>> d_resultCache;
};

class NegativeCachingRedisClient : public RedisKVClientInterface
{
public:
  NegativeCachingRedisClient(std::unique_ptr<RedisKVClientInterface> client, std::shared_ptr<GenericFilterInterface<std::string>> cache) :
    d_client(std::move(client)), d_negativeCache(cache)
  {
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::unique_ptr<RedisKVClientInterface> d_client;
  std::shared_ptr<GenericFilterInterface<std::string>> d_negativeCache;
};

// TODO: consider trie storage here
class CopyCache : public GenericCacheInterface<std::string, std::string>
{
public:
  CopyCache(unsigned int ttl) :
    d_ttl(ttl)
  {
    d_stats.d_memoryUsed = sizeof(*this);
  }
  void insert(const std::string& key, std::string value) override;
  void insertKey(const std::string& key) override;
  bool getValue(const std::string& key, std::string& value) override;
  bool contains(const std::string& key) override;
  bool remove(const std::string& key) override;
  size_t purgeExpired(size_t upTo, const time_t now) override;
  size_t expunge(size_t upTo = 0) override;
  [[nodiscard]] const GenericCacheInterface<std::string, std::string>::Stats& getStats() const override;

  bool needsUpdate();
  void insertBatch(std::unordered_map<std::string, std::string> batch);

private:
  SharedLockGuarded<std::unordered_map<std::string, std::string>> d_map{};
  const unsigned int d_ttl;
  unsigned int d_lastInsert;
  GenericCacheInterface<std::string, std::string>::Stats d_stats{"filter=\"none\""};
};

class CopyCachingRedisClient : public RedisKVClientInterface
{
public:
  CopyCachingRedisClient(std::unique_ptr<RedisKVClientInterface> client, unsigned int cacheTtl) :
    d_client(std::move(client))
  {
    d_copyCache = std::make_shared<CopyCache>(cacheTtl);
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::unique_ptr<RedisKVClientInterface> d_client;
  std::shared_ptr<CopyCache> d_copyCache;
};

class FilteringCopyCachingRedisClient : public RedisKVClientInterface
{
public:
  FilteringCopyCachingRedisClient(std::unique_ptr<RedisKVClientInterface> client, unsigned int cacheTtl, std::shared_ptr<GenericFilterInterface<std::string>> filter) :
    d_client(std::move(client)), d_copyCacheFilter(filter), d_ttl(cacheTtl)
  {
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::unique_ptr<RedisKVClientInterface> d_client;
  std::shared_ptr<GenericFilterInterface<std::string>> d_copyCacheFilter;
  const unsigned int d_ttl;
  unsigned int d_lastInsert;
};

class RedisKVClient : public RedisKVClientInterface
{
public:
  // TODO: use something else for data-name label maybe?
  // TODO: maybe take in the stats reference for simpler labels setup?
  RedisKVClient(const std::shared_ptr<RedisClient>& client, std::unique_ptr<RedisLookupAction> lookupAction, std::shared_ptr<RedisStats> stats) :
    d_client(client), d_lookupAction(std::move(lookupAction)), d_stats(stats)
  {
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::shared_ptr<RedisClient>
    d_client;
  std::unique_ptr<RedisLookupAction> d_lookupAction;
  std::shared_ptr<RedisStats> d_stats;
};
