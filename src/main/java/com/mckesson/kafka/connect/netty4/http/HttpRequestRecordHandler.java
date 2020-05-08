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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.connect.netty4.SourceRecordHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class HttpRequestRecordHandler extends SourceRecordHandler implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestRecordHandler.class);

  public static final String AUTH_CONFIG = "authValue";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(AUTH_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, "Required Authorization header value");

  private Password authorizationLine;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    authorizationLine = conf.getPassword(AUTH_CONFIG);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object o) throws Exception {

    if (o instanceof HttpRequest) {
      HttpRequest msg = (HttpRequest) o;
      if (msg == null) {
        return;
      }

      final HttpResponse response;
      if (authorizationLine == null ||
          (msg.headers().contains(HttpHeaders.AUTHORIZATION) && authorizationLine.value().equals(msg.headers().get(HttpHeaders.AUTHORIZATION)))) {
        response = createResponse(HttpResponseStatus.OK);
        List<SourceRecord> records = produceRecords(msg);
        LOG.debug("Queued for Topic: {}, records: {}", topic, records.size());
        recordQueue.addAll(records);
      } else {
        response = createResponse(HttpResponseStatus.UNAUTHORIZED);
      }

      // Write the response.
      ChannelFuture future = ctx.write(response);
      future.addListener(ChannelFutureListener.CLOSE); // WE DO NOT RESPECT Keep-Alive
    }

  }

  protected HttpResponse createResponse(HttpResponseStatus status) {
    return new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
  }

  protected List<SourceRecord> produceRecords(HttpRequest msg) throws Exception {

    if (msg instanceof ByteBufHolder) {
      ByteBuf dataBuffer = ((ByteBufHolder) msg).content();
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

      Map<String, ?> sourcePartition = new HashMap<>();
      Map<String, ?> sourceOffset = new HashMap<>();
      LOG.trace("Read bytes:{}", data.length);
      return Arrays.asList(new SourceRecord(sourcePartition, sourceOffset, topic, null, data));
    }

    Map<String, ?> sourcePartition = new HashMap<>();
    Map<String, ?> sourceOffset = new HashMap<>();

    return Arrays.asList(new SourceRecord(sourcePartition, sourceOffset, topic, null, msg.uri()));

  }

}
