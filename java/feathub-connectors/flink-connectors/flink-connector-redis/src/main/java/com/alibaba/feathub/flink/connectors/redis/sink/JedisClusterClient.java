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

import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A subclass of {@link JedisClient} that connects to a Redis cluster. Used for Redis Cluster mode.
 */
public class JedisClusterClient implements JedisClient {
    private static final byte[] sampleKey = "sampleKey".getBytes();

    private final Set<HostAndPort> nodes;
    private final String username;
    private final String password;

    private transient JedisCluster cluster;

    private transient BidiMap scriptShaMap;

    public JedisClusterClient(Set<HostAndPort> nodes, String username, String password) {
        this.nodes = nodes;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open() {
        cluster = new JedisCluster(this.nodes, this.username, this.password);
        scriptShaMap = new DualHashBidiMap();
    }

    @Override
    public long hset(byte[] key, Map<byte[], byte[]> hash) {
        return this.cluster.hset(key, hash);
    }

    @Override
    public byte[] scriptLoad(byte[] script) {
        if (scriptShaMap.containsValue(script)) {
            return (byte[]) scriptShaMap.getKey(script);
        }
        byte[] result = this.cluster.scriptLoad(script, sampleKey);
        scriptShaMap.put(result, script);
        return result;
    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        try {
            return this.cluster.evalsha(sha1, keys, args);
        } catch (Exception e) {
            // The relevant script might have not been loaded on all cluster nodes yet.
            if (!scriptShaMap.containsKey(sha1)) {
                throw e;
            }
            byte[] script = (byte[]) scriptShaMap.get(sha1);
            for (byte[] key : keys) {
                byte[] sha = this.cluster.scriptLoad(script, key);
                Preconditions.checkState(Arrays.equals(sha1, sha));
            }
            return this.cluster.evalsha(sha1, keys, args);
        }
    }

    @Override
    public void close() {
        this.cluster.close();
    }
}
