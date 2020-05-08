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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpSourceTask extends NettySourceTask {

  public static final String UPD_OPTIONS = "transport.protocol.udp.";

  @Override
  protected Channel createWorkerChannel(InetAddress bindAddress, List<Integer> ports, EventLoopGroup bossGroup, EventLoopGroup workerGroup, ChannelInitializer channelInitializer) {
    InetSocketAddress addr = selectUdpSocketAddress(bindAddress, ports);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
        .handler(channelInitializer)
        .channel(NioDatagramChannel.class);

    bootstrap.option(ChannelOption.SO_RCVBUF, 2048);

    //TODO inplement options 
    /**
    Map<String, Object> options = this.connConfig.originalsWithPrefix(UPD_OPTIONS);
    if (options != null && options.size() > 0) {
      bootstrap.setOptions(options);
    }
    */

    // Bind and start to accept incoming connections.
    Channel ch = bootstrap.bind(addr).channel();
    return ch;
  }

  @Override
  protected Class<? extends NettySourceChannelInitializer> getDefaultChannelInitializerClass() {
    return DefaultUdpChannelInitializer.class;
  }
}
