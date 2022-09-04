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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for buffer entity.
 */
public class BufferEntityMapSerializer implements SimpleVersionedSerializer<Map<String, StarRocksSinkBufferEntity>> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Map<String, StarRocksSinkBufferEntity> map) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);

        out.writeInt(map.size());
        for (Map.Entry<String, StarRocksSinkBufferEntity> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            serializeBufferEntity(out, entry.getValue());
        }

        return out.getCopyOfBuffer();
    }

    @Override
    public Map<String, StarRocksSinkBufferEntity> deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        int size = in.readInt();
        Map<String, StarRocksSinkBufferEntity> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String str = in.readUTF();
            StarRocksSinkBufferEntity entity = deserializeBufferEntity(in);
            map.put(str, entity);
        }
        return map;
    }

    private void serializeBufferEntity(DataOutputSerializer out, StarRocksSinkBufferEntity entity) throws IOException {
        out.writeInt(entity.getBatchCount());
        out.writeLong(entity.getBatchSize());
        out.writeUTF(entity.getLabel());
        out.writeUTF(entity.getDatabase());
        out.writeUTF(entity.getTable());
        out.writeBoolean(entity.EOF());
        out.writeUTF(entity.getLabelPrefix());
        ArrayList<byte[]> buffer = entity.getBuffer();
        out.writeInt(buffer.size());
        for (byte[] data : buffer) {
            out.writeInt(data.length);
            out.write(data);
        }
    }

    private StarRocksSinkBufferEntity deserializeBufferEntity(DataInputDeserializer in) throws IOException {
        int batchCount = in.readInt();
        long batchSize = in.readLong();
        String label = in.readUTF();
        String database = in.readUTF();
        String table = in.readUTF();
        boolean eof = in.readBoolean();
        String labelPrefix = in.readUTF();
        int bufferSize = in.readInt();
        ArrayList<byte[]> buffer = new ArrayList<>(bufferSize);
        for (int i = 0; i < bufferSize; i++) {
            int size = in.readInt();
            byte[] data = new byte[size];
            in.readFully(data);
            buffer.add(data);
        }

        return StarRocksSinkBufferEntity.of(
                buffer, batchCount, batchSize, label, database, table, eof, labelPrefix);
    }
}
