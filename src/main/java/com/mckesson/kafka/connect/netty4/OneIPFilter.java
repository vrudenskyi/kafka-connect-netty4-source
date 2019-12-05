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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ipfilter.AbstractRemoteAddressFilter;

@ChannelHandler.Sharable
public class OneIPFilter extends AbstractRemoteAddressFilter<InetSocketAddress> {

  private final Map<InetAddress, AtomicInteger> connected = new ConcurrentHashMap<InetAddress, AtomicInteger>();

  @Override
  protected boolean accept(ChannelHandlerContext ctx, InetSocketAddress remoteAddress) throws Exception {
    final InetAddress remoteIp = remoteAddress.getAddress();
    if (connected.containsKey(remoteIp)) {
      return false;
    } else {
      ctx.channel().closeFuture().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          connected.remove(remoteIp);
        }
      });
      return true;
    }
  }
}