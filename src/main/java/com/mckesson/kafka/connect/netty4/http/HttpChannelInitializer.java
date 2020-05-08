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

import java.util.LinkedHashMap;

import com.mckesson.kafka.connect.netty4.NettySourceChannelInitializer;
import com.mckesson.kafka.connect.netty4.NettySourceConnectorConfig;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpChannelInitializer extends NettySourceChannelInitializer {

  @Override
  public LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySourceConnectorConfig conf) {
    LinkedHashMap<String, ChannelHandler> defaultHandlers = new LinkedHashMap<>();

    //p.addLast(new HttpServerCodec());
    //p.addLast(new HttpServerExpectContinueHandler());
    //defaultHandlers.put("httpCodec", new HttpServerCodec());

    defaultHandlers.put("decoder", new HttpRequestDecoder());
    defaultHandlers.put("decoder_compress", new HttpContentDecompressor());
    defaultHandlers.put("aggregator", new HttpObjectAggregator(32 * 1024 * 1024));
    defaultHandlers.put("encoder", new HttpResponseEncoder());
    HttpRequestRecordHandler recordHandler = new HttpRequestRecordHandler();
    recordHandler.setTopic(topic);
    recordHandler.setRecordQueue(messageQueue);
    defaultHandlers.put("recordHandler", recordHandler);
    return defaultHandlers;
  }

}
