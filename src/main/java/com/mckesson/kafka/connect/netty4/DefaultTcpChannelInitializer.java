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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.ReadTimeoutHandler;

public class DefaultTcpChannelInitializer extends NettySourceChannelInitializer {

  private static final String FRAME_CONFIG_PREFIX = NettySourceConnectorConfig.CHANNEL_CONFIG + ".tcp.frame.";
  public static final String MAX_LENGTH_CONFIG = FRAME_CONFIG_PREFIX + "maxLength";
  public static final int MAX_LENGTH_DEFAULT = 8192;

  public static final String STRIP_DELIMETER_CONFIG = FRAME_CONFIG_PREFIX + "stripDelimiter";
  private static final Boolean STRIP_DELIMETER_DEFAULT = Boolean.TRUE;

  public static final String FAIL_FAST_CONFIG = FRAME_CONFIG_PREFIX + "failFast";
  private static final Boolean FAIL_FAST_DEFAULT = Boolean.FALSE;

  public static final String DELIMETERS_CONFIG = FRAME_CONFIG_PREFIX + "delimeters";
  public static final List<String> DELIMETERS_DEFAULT = Arrays.asList("\\0", "\\n");

  public static final String NODATA_TIMEOUT_CONFIG = NettySourceConnectorConfig.CHANNEL_CONFIG + ".tcp.nodataTimeout";
  private static final Long NODATA_TIMEOUT_DEFAULT = 0L;

  public static final String MAX_CONNECTIONS_CONFIG = NettySourceConnectorConfig.CHANNEL_CONFIG + ".tcp.maxConnections";
  private static final int MAX_CONNECTIONS_DEFAULT = 0;

  public static final String MAX_CONNECTIONS_PER_IP_CONFIG = NettySourceConnectorConfig.CHANNEL_CONFIG + ".tcp.maxConnectionsPerIP";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(MAX_LENGTH_CONFIG, ConfigDef.Type.INT, MAX_LENGTH_DEFAULT, ConfigDef.Importance.MEDIUM, "Max Message Length")
      .define(STRIP_DELIMETER_CONFIG, ConfigDef.Type.BOOLEAN, STRIP_DELIMETER_DEFAULT, ConfigDef.Importance.MEDIUM, "whether the decoded frame should strip out the delimiter or not")
      .define(FAIL_FAST_CONFIG, ConfigDef.Type.BOOLEAN, FAIL_FAST_DEFAULT, ConfigDef.Importance.MEDIUM, "see LineBasedFrameDecoder javadoc")
      .define(DELIMETERS_CONFIG, ConfigDef.Type.LIST, DELIMETERS_DEFAULT, ConfigDef.Importance.MEDIUM, "list of delimeter strings")
      .define(NODATA_TIMEOUT_CONFIG, ConfigDef.Type.LONG, NODATA_TIMEOUT_DEFAULT, ConfigDef.Importance.MEDIUM, "when no data was read within a certain period of time")
      .define(MAX_CONNECTIONS_CONFIG, ConfigDef.Type.INT, MAX_CONNECTIONS_DEFAULT, ConfigDef.Importance.MEDIUM, "max number of connections allowed. default 4096. set to 0 to disable")
      .define(MAX_CONNECTIONS_PER_IP_CONFIG, ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM, "max number of connections per IP allowed. default: maxConnections");

  private int maxLength = 8192;
  private boolean stripDelimiter = true;
  private boolean failFast = false;
  private List<ByteBuf> delimeters;

  private ReadTimeoutHandler readTimeoutHandler;
  private ConnectionsCountFilterHandler connectionsCountHandler;

  public LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySourceConnectorConfig conf) {

    LinkedHashMap<String, ChannelHandler> defaultHandlers = new LinkedHashMap<>();

    if (readTimeoutHandler != null) {
      defaultHandlers.put("nodataTimeout", readTimeoutHandler);
    }

    if (connectionsCountHandler != null) {
      defaultHandlers.put("connectionsCount", connectionsCountHandler);
    }

    defaultHandlers.put("framer", new DelimeterOrMaxLengthFrameDecoder(maxLength, stripDelimiter, failFast, delimeters.toArray(new ByteBuf[0])));
    defaultHandlers.put("decoder", new StringDecoder());
    StringRecordHandler handler = new StringRecordHandler();
    handler.setTopic(topic);
    handler.setRecordQueue(messageQueue);
    defaultHandlers.put("recordHandler", handler);
    return defaultHandlers;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    SimpleConfig syslogConfig = new SimpleConfig(CONFIG_DEF, configs);
    this.maxLength = syslogConfig.getInt(MAX_LENGTH_CONFIG);
    this.failFast = syslogConfig.getBoolean(FAIL_FAST_CONFIG);
    this.stripDelimiter = syslogConfig.getBoolean(STRIP_DELIMETER_CONFIG);
    List<String> dlStrings = syslogConfig.getList(DELIMETERS_CONFIG);
    if (dlStrings != null && dlStrings.size() > 0) {
      this.delimeters = new ArrayList<>(dlStrings.size());
      for (String dl : dlStrings) {
        String str = StringEscapeUtils.unescapeJava(dl);
        this.delimeters.add(Unpooled.wrappedBuffer(str.getBytes()));
      }
    }
    long readTimeout = syslogConfig.getLong(NODATA_TIMEOUT_CONFIG);
    if (readTimeout > 0) {
      this.readTimeoutHandler = new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS);
    }

    //TODO

    //    Integer maxConnections = syslogConfig.getInt(MAX_CONNECTIONS_CONFIG);
    //    Integer maxConnectionsPerIP = syslogConfig.getInt(MAX_CONNECTIONS_PER_IP_CONFIG);
    //    if (maxConnections > 0) {
    //      this.connectionsCountHandler = new ConnectionsCountFilterHandler(maxConnections, maxConnectionsPerIP == null ? maxConnections : maxConnectionsPerIP);
    //    }

  }

}
