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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ipfilter.AbstractRemoteAddressFilter;

/**
 *inspired by  {@link OneIpFilterHandler}
 * 
 * @author ew4ahmz
 */
public class ConnectionsCountFilterHandler extends AbstractRemoteAddressFilter<InetSocketAddress> {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionsCountFilterHandler.class);

  /** HashMap of current remote connected InetAddress */
  private final ConcurrentMap<InetAddress, AtomicInteger> connectedSet = new ConcurrentHashMap<InetAddress, AtomicInteger>();
  private final AtomicInteger allConnectionsCount = new AtomicInteger();
  private final int maxConnectionsPerIP = -1;
  private final int maxConnections = -1;

  @Override
  protected boolean accept(ChannelHandlerContext ctx, InetSocketAddress remoteAddress) throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  /***  
  
  public ConnectionsCountFilterHandler() {
    this(65535, 65535);
  }
  
  public ConnectionsCountFilterHandler(int maxConnections) {
    this(maxConnections, maxConnections);
  }
  
  public ConnectionsCountFilterHandler(int maxConnections, int maxConnectionsPerIP) {
    this.maxConnections = maxConnections;
    this.maxConnectionsPerIP = maxConnectionsPerIP;
  }
  
  @Override
  protected boolean accept(ChannelHandlerContext ctx, ChannelEvent e, InetSocketAddress inetSocketAddress)
      throws Exception {
    InetAddress inetAddress = inetSocketAddress.getAddress();
    if (!connectedSet.containsKey(inetAddress)) {
      connectedSet.put(inetAddress, new AtomicInteger(0));
    }
  
    AtomicInteger perIpCount = connectedSet.get(inetAddress);
    perIpCount.incrementAndGet();
    allConnectionsCount.incrementAndGet();
  
    if (perIpCount.get() > maxConnectionsPerIP || allConnectionsCount.get() > maxConnections) {
      LOG.warn("max number of allowed connectors exeeded. TOTAL: {} of {}, {}: {} of {}", 
          allConnectionsCount.get(), maxConnections,
          inetAddress.getHostAddress(), perIpCount, maxConnectionsPerIP);
      return false;
    }
  
    return true;
  }
  
  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    super.handleUpstream(ctx, e);
    // Try to remove entry from Map if already exists
    if (e instanceof ChannelStateEvent) {
      ChannelStateEvent evt = (ChannelStateEvent) e;
      if (evt.getState() == ChannelState.CONNECTED) {
        if (evt.getValue() == null) {
          // DISCONNECTED but was this channel blocked or not
          if (isBlocked(ctx)) {
            // remove inetsocketaddress from set since this channel was not blocked before
            InetSocketAddress inetSocketAddress = (InetSocketAddress) e.getChannel().getRemoteAddress();
            allConnectionsCount.decrementAndGet();
            int perIpCount = connectedSet.getOrDefault(inetSocketAddress, new AtomicInteger()).decrementAndGet();
            //remove IP with 0 conns just to keep connectedSet clean
            if (perIpCount <= 0) {
              connectedSet.remove(inetSocketAddress);
            }
          }
        }
      }
    }
  }
  ***/
}
