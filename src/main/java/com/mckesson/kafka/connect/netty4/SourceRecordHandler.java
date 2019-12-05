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
package com.mckesson.kafka.connect.netty4;

import java.util.concurrent.BlockingQueue;

import org.apache.kafka.connect.source.SourceRecord;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Base class for message handling ChannelHandlers
 */
public abstract class SourceRecordHandler<T> extends SimpleChannelInboundHandler<T> {

  protected BlockingQueue<SourceRecord> recordQueue;
  protected String topic;

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setRecordQueue(BlockingQueue<SourceRecord> queue) {
    this.recordQueue = queue;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }

}
