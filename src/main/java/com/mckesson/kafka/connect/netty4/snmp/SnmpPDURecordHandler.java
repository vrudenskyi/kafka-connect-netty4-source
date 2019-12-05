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
package com.mckesson.kafka.connect.netty4.snmp;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.PDU;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.kafka.connect.netty4.SourceRecordHandler;

import io.netty.channel.ChannelHandlerContext;

public class SnmpPDURecordHandler extends SourceRecordHandler<PDU> implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(SnmpPDURecordHandler.class);

  private PDUConverter conv;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs) {
    conv = new PDUConverter();
    conv.configure(configs);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, PDU pdu) throws Exception {

    LOG.trace("received PDU: {}", pdu);
    try {
      if (recordQueue != null) {
        Map<String, Object> pduData = conv.convert(pdu);

        Map<String, ?> sourcePartition = new HashMap<>();
        Map<String, ?> sourceOffset = new HashMap<>();
        SourceRecord srcRec = new SourceRecord(sourcePartition, sourceOffset, topic, null, OBJECT_MAPPER.writeValueAsString(pduData));
        recordQueue.add(srcRec);
      } else {
        LOG.error("RecordQueue is not configured!!!!");
      }
    } catch (Throwable t) {
      LOG.error("failed to convert PDU", t);
    }

  }

}
