/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.table.sink.BufferEntityMapSerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests for {@link BufferEntityMapSerializer}. */
public class BufferEntityMapSerializerTest {

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        Random random = new Random();
        int size = random.nextInt(20);
        Map<String, StarRocksSinkBufferEntity> originalData = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String name = generateRandomStr(random, random.nextInt(30) + 1);
            StarRocksSinkBufferEntity entity = generateEntity(random);
            originalData.put(name, entity);
        }

        BufferEntityMapSerializer serializer = new BufferEntityMapSerializer();
        byte[] data = serializer.serialize(originalData);
        Map<String, StarRocksSinkBufferEntity> actualData = serializer.deserialize(1, data);

        assertEquals(originalData.size(), actualData.size());
        for (Map.Entry<String, StarRocksSinkBufferEntity> entry : originalData.entrySet()) {
            StarRocksSinkBufferEntity entity = actualData.get(entry.getKey());
            assertNotNull(entity);
            assertEqualBufferEntity(entry.getValue(), entity);
        }
    }

    private StarRocksSinkBufferEntity generateEntity(Random random) {
        StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(
                generateRandomStr(random, random.nextInt(10) + 1),
                generateRandomStr(random, random.nextInt(20) + 1),
                generateRandomStr(random, random.nextInt(15) + 1)
            );
        int batchCount = random.nextInt(10);
        for (int i = 0; i < batchCount; i++) {
            byte[] data = new byte[random.nextInt(50) + 1];
            random.nextBytes(data);
            entity.addToBuffer(data);
        }
        if (random.nextBoolean()) {
            entity.asEOF();
        }
        return entity;
    }

    private String generateRandomStr(Random random, int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return new String(data);
    }

    private void assertEqualBufferEntity(StarRocksSinkBufferEntity expect, StarRocksSinkBufferEntity actual) {
        assertEquals(expect.getDatabase(), actual.getDatabase());
        assertEquals(expect.getTable(), actual.getTable());
        assertEquals(expect.getLabelPrefix(), actual.getLabelPrefix());
        assertEquals(expect.getBatchCount(), actual.getBatchCount());
        assertEquals(expect.getBatchSize(), actual.getBatchSize());
        assertEquals(expect.getLabel(), actual.getLabel());
        assertEquals(expect.EOF(), actual.EOF());
        assertEquals(expect.getBuffer().size(), actual.getBuffer().size());
        for (int i = 0; i < expect.getBuffer().size(); i++) {
            assertArrayEquals(expect.getBuffer().get(i), actual.getBuffer().get(i));
        }
    }
}
