/*
 * Copyright 2022 The FeatHub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.connectors.redis.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import redis.clients.jedis.HostAndPort;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.DB_NUM;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.NODE_URLS;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.PASSWORD;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.REDIS_MODE;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.USERNAME;

/** A wrapper interface for jedis clients in different deployment modes. */
public interface JedisClient {
    long hset(final byte[] key, final Map<byte[], byte[]> hash);

    byte[] scriptLoad(final byte[] script);

    Object evalsha(final byte[] sha1, final List<byte[]> keys, final List<byte[]> args);

    void close();

    static JedisClient create(ReadableConfig config) throws MalformedURLException {
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        if (Objects.requireNonNull(config.get(REDIS_MODE)) == RedisSinkConfigs.RedisMode.CLUSTER) {
            Set<HostAndPort> nodes = new HashSet<>();
            for (String nodeUrl : config.get(NODE_URLS).split(";")) {
                URL url = new URL(nodeUrl);
                String host = url.getHost();
                int port = url.getPort();
                nodes.add(new HostAndPort(host, port));
            }
            return new JedisClusterClient(nodes, username, password);
        }
        int dbNum = config.get(DB_NUM);
        Preconditions.checkArgument(config.get(NODE_URLS).split(";").length == 1);
        URL url = new URL(config.get(NODE_URLS));
        String host = url.getHost();
        int port = url.getPort();
        return new JedisMasterClient(host, port, username, password, dbNum);
    }
}
