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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TcpSourceTask extends NettySourceTask {

  private static final Logger log = LoggerFactory.getLogger(TcpSourceTask.class);

  @Override
  protected Channel createWorkerChannel(InetAddress bindAddress, List<Integer> ports, EventLoopGroup bossGroup, EventLoopGroup workerGroup, ChannelInitializer channelInitializer) {
    InetSocketAddress addr = selectTcpSocketAddress(bindAddress, ports);
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
              .childHandler(channelInitializer)
              .channel(NioServerSocketChannel.class);
    // Bind and start to accept incoming connections.
    Channel ch = bootstrap.bind(addr).channel();
    log.debug("started listening  on: {}", addr);
    return ch;
  }

  @Override
  protected Class<? extends NettySourceChannelInitializer> getDefaultChannelInitializerClass() {
    return DefaultTcpChannelInitializer.class;
  }
}
