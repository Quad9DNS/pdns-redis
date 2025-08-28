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

#include "dolog.hh"
#include "generic-cache.hh"
#include "iputils.hh"
#include "channel.hh"
#include "lock.hh"
#include <thread>
#include <yahttp/yahttp.hpp>
#include <hiredis/hiredis.h>
#include <memory>
#include <string>

using cache_t = std::unordered_map<std::string, std::string>;
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
    return d_reply && d_reply->type == REDIS_REPLY_ARRAY && d_reply->elements == 2 && d_reply->element[0]->type == REDIS_REPLY_BIGNUM;
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

  private:
    mutable LockGuarded<std::unique_ptr<redisContext, decltype(&redisFree)>> d_context{std::unique_ptr<redisContext, decltype(&redisFree)>(nullptr, redisFree)};
    YaHTTP::URL d_url;
  };

  class Executor
  {
  public:
    virtual ~Executor() = default;
    virtual redisReply* executeCommand(const char* format, va_list ap) const = 0;
  };

  class DirectExecutor : public Executor
  {
  public:
    DirectExecutor(const std::string& url) :
      d_connection(url)
    {
    }
    redisReply* executeCommand(const char* format, va_list ap) const override;

  private:
    RedisConnection d_connection;
  };

  class PipelineExecutor : public Executor
  {
  public:
    PipelineExecutor(const std::string& url, uint32_t pipelineInterval);
    ~PipelineExecutor();
    redisReply* executeCommand(const char* format, va_list ap) const override;

  private:
    void maintenanceThread();

    struct PipelineCommand
    {
      typedef std::function<void(redisReply*)> callback_t;
      char* command;
      int length;
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
  NegativeCachingRedisClient(std::unique_ptr<RedisKVClientInterface> client, std::shared_ptr<GenericCacheInterface<std::string, std::string>> cache) :
    d_client(std::move(client)), d_negativeCache(cache)
  {
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::unique_ptr<RedisKVClientInterface> d_client;
  std::shared_ptr<GenericCacheInterface<std::string, std::string>> d_negativeCache;
};

class CopyCache : public GenericCacheInterface<std::string, std::string>
{
public:
  CopyCache(unsigned int ttlMs) :
    d_ttlMs(ttlMs)
  {
  }
  void insert(const std::string& key, std::string value) override;
  bool getValue(const std::string& key, std::string& value) override;
  bool contains(const std::string& key) override;
  size_t purgeExpired(size_t upTo, const time_t now) override;
  size_t expunge(size_t upTo = 0) override;

  bool needsUpdate();
  void insertBatch(std::unordered_map<std::string, std::string> batch);

private:
  SharedLockGuarded<std::unordered_map<std::string, std::string>> d_map{};
  const unsigned int d_ttlMs;
  unsigned int d_lastInsertMs;
};

class CopyCachingRedisClient : public RedisKVClientInterface
{
public:
  CopyCachingRedisClient(std::unique_ptr<RedisKVClientInterface> client, unsigned int cacheTtlMs) :
    d_client(std::move(client))
  {
    d_copyCache = std::make_shared<CopyCache>(cacheTtlMs);
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::unique_ptr<RedisKVClientInterface> d_client;
  std::shared_ptr<CopyCache> d_copyCache;
};

class RedisKVClient : public RedisKVClientInterface
{
public:
  RedisKVClient(const std::shared_ptr<RedisClient>& client, std::unique_ptr<RedisLookupAction> lookupAction = std::make_unique<RedisGetLookupAction>()) :
    d_client(client), d_lookupAction(std::move(lookupAction))
  {
  }

  bool getValue(const std::string& key, std::string& value) override;
  std::unordered_map<std::string, std::string> generateCopyCache() override;
  bool keyExists(const std::string& key) override;

private:
  std::shared_ptr<RedisClient> d_client;
  std::unique_ptr<RedisLookupAction> d_lookupAction;
};
