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
#include "gettime.hh"
#include "threadname.hh"
#include <condition_variable>
#include <stdexcept>
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "redis.hh"
#include "dolog.hh"
#include <hiredis/hiredis.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

std::unique_ptr<RedisReplyInterface<std::string>> RedisGetCommand::operator()(const RedisClient& client, const std::string& key) const
{
  return std::make_unique<RedisStringReply>(client.executeCommand("GET %b", key.data(), key.length()));
}

std::unique_ptr<RedisReplyInterface<bool>> RedisExistsCommand::operator()(const RedisClient& client, const std::string& key) const
{
  return std::make_unique<RedisIntAsBoolReply>(std::make_unique<RedisIntReply>(client.executeCommand("EXISTS %b", key.data(), key.length())));
}

std::unique_ptr<RedisReplyInterface<std::string>> RedisHGetCommand::operator()(const RedisClient& client, const std::string& hash_key, const std::string& key) const
{
  return std::make_unique<RedisStringReply>(client.executeCommand("HGET %b %b", hash_key.data(), hash_key.length(), key.data(), key.length()));
}

std::unique_ptr<RedisReplyInterface<std::unordered_map<std::string, std::string>>> RedisHGetAllCommand::operator()(const RedisClient& client, const std::string& hash_key) const
{
  return std::make_unique<RedisHashReply>(client.executeCommand("HGETALL %b", hash_key.data(), hash_key.length()));
}

std::unique_ptr<RedisReplyInterface<bool>> RedisHExistsCommand::operator()(const RedisClient& client, const std::string& hash_key, const std::string& key) const
{
  return std::make_unique<RedisIntAsBoolReply>(std::make_unique<RedisIntReply>(client.executeCommand("HEXISTS %b %b", hash_key.data(), hash_key.length(), key.data(), key.length())));
}

std::unique_ptr<RedisReplyInterface<bool>> RedisSIsMemberCommand::operator()(const RedisClient& client, const std::string& set_key, const std::string& key) const
{
  return std::make_unique<RedisIntAsBoolReply>(std::make_unique<RedisIntReply>(client.executeCommand("SISMEMBER %b %b", set_key.data(), set_key.length(), key.data(), key.length())));
}

std::unique_ptr<RedisReplyInterface<std::unordered_set<std::string>>> RedisSMembersCommand::operator()(const RedisClient& client, const std::string& set_key) const
{
  return std::make_unique<RedisSetReply>(client.executeCommand("SMEMBERS %b", set_key.data(), set_key.length()));
}

std::unique_ptr<RedisReplyInterface<bool>> RedisSScanCommand::operator()(const RedisClient& client, const std::string& set_key, const size_t& cursor, const std::string& key, const size_t& count) const
{
  return std::make_unique<RedisScanAsBoolReply>(client.executeCommand("SSCAN %b %d %b %d", set_key.data(), set_key.length(), cursor, key.data(), key.length(), count));
}

std::unique_ptr<RedisReplyInterface<std::unordered_set<std::string>>> RedisZrangeBylexCommand::operator()(const RedisClient& client, const std::string& set_key, const size_t& start, const size_t& stop) const
{
  return std::make_unique<RedisSetReply>(client.executeCommand("ZRANGE %b %d %d BYLEX", set_key.data(), set_key.length(), start, stop));
}

std::unique_ptr<RedisReplyInterface<std::string>> RedisGetLookupAction::getValue(const RedisClient& client, const std::string& key) const
{
  return d_getCommand(client, d_prefix + key);
}

std::unique_ptr<RedisReplyInterface<bool>> RedisGetLookupAction::keyExists(const RedisClient& client, const std::string& key) const
{
  return d_existsCommand(client, d_prefix + key);
}

std::unordered_map<std::string, std::string> RedisGetLookupAction::generateCopyCache([[maybe_unused]] const RedisClient& client) const
{
  return {};
}

bool RedisGetLookupAction::getFromCopyCache([[maybe_unused]] GenericCacheInterface<std::string, std::string>& cache, [[maybe_unused]] const std::string& key, [[maybe_unused]] std::string& value) const
{
  return false;
}

std::unique_ptr<RedisReplyInterface<std::string>> RedisHGetLookupAction::getValue(const RedisClient& client, const std::string& key) const
{
  return d_getCommand(client, d_hash_key, key);
}

std::unique_ptr<RedisReplyInterface<bool>> RedisHGetLookupAction::keyExists(const RedisClient& client, const std::string& key) const
{
  return d_existsCommand(client, d_hash_key, key);
}

std::unordered_map<std::string, std::string> RedisHGetLookupAction::generateCopyCache(const RedisClient& client) const
{
  return d_getAllCommand(client, d_hash_key)->getValue();
}

bool RedisHGetLookupAction::getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const
{
  return cache.getValue(key, value);
}

std::unique_ptr<RedisReplyInterface<std::string>> RedisSismemberLookupAction::getValue(const RedisClient& client, const std::string& key) const
{
  return std::make_unique<RedisBoolAsStringReply>(d_sIsMemberCommand(client, d_set_key, key));
}

std::unique_ptr<RedisReplyInterface<bool>> RedisSismemberLookupAction::keyExists(const RedisClient& client, const std::string& key) const
{
  return d_sIsMemberCommand(client, d_set_key, key);
}

std::unordered_map<std::string, std::string> RedisSismemberLookupAction::generateCopyCache(const RedisClient& client) const
{
  auto elements = d_sMembersCommand(client, d_set_key)->getValue();
  std::unordered_map<std::string, std::string> result{elements.size()};
  for (auto element : elements) {
    result.emplace(element, "1");
  }
  return result;
}

bool RedisSismemberLookupAction::getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const
{
  if (cache.contains(key)) {
    value = "1";
    return true;
  }
  return false;
}

std::unique_ptr<RedisReplyInterface<std::string>> RedisSscanLookupAction::getValue(const RedisClient& client, const std::string& key) const
{
  return std::make_unique<RedisScanAsStringReply>(client.executeCommand("SSCAN %b 0 %b", d_set_key.data(), d_set_key.length(), key.data(), key.length()));
}

std::unique_ptr<RedisReplyInterface<bool>> RedisSscanLookupAction::keyExists(const RedisClient& client, const std::string& key) const
{
  return std::make_unique<RedisScanAsBoolReply>(client.executeCommand("SSCAN %b 0 %b", d_set_key.data(), d_set_key.length(), key.data(), key.length()));
}

std::unordered_map<std::string, std::string> RedisSscanLookupAction::generateCopyCache(const RedisClient& client) const
{
  RedisSetReply reply{client.executeCommand("SMEMBERS %b", d_set_key.data(), d_set_key.length())};
  auto elements = reply.getValue();
  std::unordered_map<std::string, std::string> result{elements.size()};
  for (auto element : elements) {
    result.emplace(element, "1");
  }
  return result;
}

bool RedisSscanLookupAction::getFromCopyCache(GenericCacheInterface<std::string, std::string>& cache, const std::string& key, std::string& value) const
{
  if (cache.contains(key)) {
    value = "1";
    return true;
  }
  return false;
}

redisReply* RedisClient::executeCommand(const char* format, ...) const
{
  va_list ap;
  va_start(ap, format);
  auto result = d_executor->executeCommand(format, ap);
  va_end(ap);
  return result;
};

redisReply* RedisClient::DirectExecutor::executeCommand(const char* format, va_list ap) const
{
  auto connection = d_connection.getConnection();
  auto result = static_cast<redisReply*>(redisvCommand(connection->get(), format, ap));
  if (connection->get()->err != 0) {
    vinfolog("Redis connection error %s", connection->get()->errstr);
  }
  return result;
}

RedisClient::PipelineExecutor::PipelineExecutor(const std::string& url, uint32_t pipelineInterval) :
  d_connection(url), d_interval(pipelineInterval)
{
  auto [sender, receiver] = pdns::channel::createObjectQueue<PipelineCommand>();
  d_pipelineSender = std::move(sender);
  d_pipelineReceiver = std::move(receiver);

  d_thread = std::thread(&RedisClient::PipelineExecutor::maintenanceThread, this);
}

redisReply* RedisClient::PipelineExecutor::executeCommand(const char* format, va_list ap) const
{
  char* command;
  auto len = redisvFormatCommand(&command, format, ap);
  if (len < 0) {
    // TODO: handle formatting errors?
    return nullptr;
  }

  std::mutex mtx;
  std::condition_variable cv;
  std::unique_lock<std::mutex> lock(mtx);
  std::shared_ptr<redisReply*> result = std::make_shared<redisReply*>(nullptr);
  PipelineCommand::callback_t callback = [result, &cv](redisReply* reply) mutable {
    *result = reply;
    cv.notify_one();
  };
  d_pipelineSender.send(std::make_unique<PipelineCommand>(PipelineCommand{
    command,
    len,
    callback}));
  cv.wait(lock);
  return *result;
}

void RedisClient::PipelineExecutor::maintenanceThread()
{
  setThreadName("dnsdist/redis");

  for (;;) {
    if (d_exiting) {
      break;
    }

    bool connected = true;
    if (d_connection.needsReconnect()) {
      connected = d_connection.reconnect();
    }

    if (connected) {
      auto connection = d_connection.getConnection();
      std::list<PipelineCommand::callback_t> callbacks;
      while (auto command = d_pipelineReceiver.receive()) {
        redisAppendFormattedCommand(connection->get(), command->get()->command, command->get()->length);
        callbacks.push_back(command->get()->callback);
      }

      for (auto callback : callbacks) {
        void* reply;
        if (redisGetReply(connection->get(), &reply) == REDIS_OK) {
          callback(static_cast<redisReply*>(reply));
        }
        else {
          if (connection->get()->err != 0) {
            vinfolog("Redis connection error %s", connection->get()->errstr);
          }
          else {
            vinfolog("Unknown redis connection error");
          }
          callback(nullptr);
        }
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(d_interval));
  }
};

RedisClient::PipelineExecutor::~PipelineExecutor()
{
  d_exiting = true;

  d_thread.join();
}

bool ResultCachingRedisClient::getValue(const std::string& key, std::string& value)
{
  if (d_resultCache->getValue(key, value)) {
    return true;
  }

  auto found = d_client->getValue(key, value);
  if (found) {
    d_resultCache->insert(key, value);
  }
  return found;
}

std::unordered_map<std::string, std::string> ResultCachingRedisClient::generateCopyCache()
{
  return d_client->generateCopyCache();
}

bool ResultCachingRedisClient::keyExists(const std::string& key)
{
  if (d_resultCache->contains(key)) {
    return true;
  }

  // No value to store in the cache here, so just return
  return d_client->keyExists(key);
}

bool NegativeCachingRedisClient::getValue(const std::string& key, std::string& value)
{
  if (d_negativeCache->contains(key)) {
    return false;
  }

  auto found = d_client->getValue(key, value);
  if (!found) {
    d_negativeCache->insertKey(key);
  }
  return found;
}

std::unordered_map<std::string, std::string> NegativeCachingRedisClient::generateCopyCache()
{
  return d_client->generateCopyCache();
}

bool NegativeCachingRedisClient::keyExists(const std::string& key)
{
  if (d_negativeCache->contains(key)) {
    return false;
  }

  auto found = d_client->keyExists(key);
  if (!found) {
    d_negativeCache->insertKey(key);
  }
  return found;
}

void CopyCache::insert(const std::string& key, std::string value)
{
  auto map = d_map.write_lock();
  map->emplace(key, value);
};

void CopyCache::insertKey([[maybe_unused]] const std::string& key)
{
  throw std::runtime_error("Unsupported insertKey operation for copy cache.");
};

bool CopyCache::getValue(const std::string& key, std::string& value)
{
  if (needsUpdate()) {
    return false;
  }

  auto map = d_map.read_lock();

  auto entry = map->find(key);
  if (entry != map->end()) {
    value = entry->second;
    return true;
  }

  return false;
};

bool CopyCache::contains(const std::string& key)
{
  auto map = d_map.read_lock();
  return map->find(key) != map->end();
};

bool CopyCache::needsUpdate()
{
  struct timespec now;
  gettime(&now);
  return d_lastInsert + d_ttl < now.tv_sec;
};

void CopyCache::insertBatch(std::unordered_map<std::string, std::string> batch)
{
  auto map = d_map.write_lock();

  map->clear();
  for (auto entry : batch) {
    map->emplace(entry);
  }
  struct timespec now;
  gettime(&now);
  d_lastInsert = now.tv_sec;
};

size_t CopyCache::purgeExpired([[maybe_unused]] size_t upTo, const time_t now)
{
  if (d_lastInsert < now - d_ttl) {
    return expunge(upTo);
  }

  return 0;
};
size_t CopyCache::expunge([[maybe_unused]] size_t upTo)
{
  auto map = d_map.write_lock();
  size_t toRemove = map->size() - upTo;

  auto beginIt = map->begin();
  auto endIt = beginIt;

  if (map->size() >= toRemove) {
    std::advance(endIt, toRemove);
    map->erase(beginIt, endIt);
    return toRemove;
  }
  else {
    map->clear();
    return map->size();
  }
};

bool CopyCachingRedisClient::getValue(const std::string& key, std::string& value)
{
  if (d_copyCache->getValue(key, value)) {
    return true;
  }

  auto found = d_client->getValue(key, value);
  if (d_copyCache->needsUpdate()) {
    d_copyCache->insertBatch(d_client->generateCopyCache());
  }
  return found;
}

std::unordered_map<std::string, std::string> CopyCachingRedisClient::generateCopyCache()
{
  return d_client->generateCopyCache();
}

bool CopyCachingRedisClient::keyExists(const std::string& key)
{
  if (d_copyCache->contains(key)) {
    return true;
  }

  // No value to store in the cache here, so just return
  return d_client->keyExists(key);
}

bool RedisKVClient::getValue(const std::string& key, std::string& value)
{
  auto reply = d_lookupAction->getValue(*d_client, key);

  if (reply->ok()) {
    value = reply->getValue();
    return true;
  }

  vinfolog("Error while looking up key '%s' from Redis: %s", key, reply->getError());
  return false;
}

std::unordered_map<std::string, std::string> RedisKVClient::generateCopyCache()
{
  return d_lookupAction->generateCopyCache(*d_client);
}

bool RedisKVClient::keyExists(const std::string& key)
{
  auto reply = d_lookupAction->keyExists(*d_client, key);
  if (reply->ok()) {
    return reply->getValue();
  }

  vinfolog("Error while looking up key '%s' from Redis: %s", key, reply->getError());
  return false;
}

namespace
{
void validateRedisUrl(const YaHTTP::URL& parsed, const std::string& url)
{
  if (parsed.protocol.empty() || (parsed.protocol != "redis" && parsed.protocol != "rediss")) {
    throw std::runtime_error("Invalid redis URL: " + url + " - Invalid protocol! Use redis or rediss.");
  }
  else if (parsed.host.empty()) {
    throw std::runtime_error("Invalid redis URL: " + url + " - Host empty.");
  }
}
}

RedisClient::RedisConnection::RedisConnection(const std::string& url)
{
  auto parsed = YaHTTP::URL();
  if (!parsed.parse(url)) {
    validateRedisUrl(parsed, url);
  }

  validateRedisUrl(parsed, url);
  d_url = parsed;

  if (parsed.port == 0) {
    parsed.port = 6379;
  }
  auto context = std::unique_ptr<redisContext, decltype(&redisFree)>(redisConnect(parsed.host.c_str(), parsed.port), redisFree);
  // Check if the context is null or if a specific
  // error occurred.
  if (context == nullptr || context->err) {
    if (context != nullptr) {
      warnlog("Error connecting to redis: %s", context->errstr);
    }
    else {
      warnlog("Can't allocate redis context");
    }
  }

  *(d_context.lock()) = std::move(context);
}

bool RedisClient::RedisConnection::reconnect()
{
  {
    auto context = d_context.read_only_lock();
    if (context->get() != nullptr) {
      int result = redisReconnect(context->get());
      return result == REDIS_OK;
    }
  }

  auto context = std::unique_ptr<redisContext, decltype(&redisFree)>(redisConnect(d_url.host.c_str(), d_url.port), redisFree);
  // Check if the context is null or if a specific
  // error occurred.
  if (context == nullptr || context->err) {
    if (context != nullptr) {
      warnlog("Error connecting to redis: %s", context->errstr);
      return false;
    }
    else {
      warnlog("Can't allocate redis context");
      return false;
    }
  }

  *(d_context.lock()) = std::move(context);
  return true;
}
