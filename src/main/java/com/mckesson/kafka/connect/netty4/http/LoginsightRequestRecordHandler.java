/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mckesson.kafka.connect.netty4.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.handler.codec.http.HttpRequest;

/**
 *  Produces Loginsight messages from HttpRequests
 */

public class LoginsightRequestRecordHandler extends HttpRequestRecordHandler {

  private ObjectMapper jsonMapper = new ObjectMapper();

  @Override
  protected List<SourceRecord> produceRecords(HttpRequest httpMsg) {

    ByteBuf dataBuffer = null;
    if (httpMsg instanceof ByteBufHolder) {
      dataBuffer = ((ByteBufHolder) httpMsg).content();
    }

    if (dataBuffer == null || !dataBuffer.isReadable()) {
      return Collections.emptyList();
    }
    byte[] data;
    if (dataBuffer.hasArray()) {
      data = dataBuffer.array();
    } else {
      int readableBytes = dataBuffer.readableBytes();
      data = new byte[readableBytes];
      dataBuffer.getBytes(0, data);
    }

    try {
      JsonNode packet = jsonMapper.readTree(data);
      JsonNode messages = packet.get("messages");
      if (messages != null && messages.isArray()) {
        List<SourceRecord> records = new ArrayList<>(messages.size());
        Map<String, ?> sourcePartition = new HashMap<>();
        Map<String, ?> sourceOffset = new HashMap<>();
        for (final JsonNode msg : messages) {
          String msgString;
          try {
            msgString = jsonMapper.writeValueAsString(msg);
          } catch (JsonProcessingException e) {
            msgString = msg.toString();
          }
          records.add(new SourceRecord(sourcePartition, sourceOffset, topic, null, msgString));
        }
        return records;
      }

    } catch (Exception e) {
      throw new ConnectException("Failed to parse incoming message", e);
    }
    return Collections.emptyList();
  }

}
