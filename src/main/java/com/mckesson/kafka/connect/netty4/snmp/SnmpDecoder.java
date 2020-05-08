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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.MutablePDU;
import org.snmp4j.PDU;
import org.snmp4j.TransportStateReference;
import org.snmp4j.asn1.BER;
import org.snmp4j.asn1.BERInputStream;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.MessageProcessingModel;
import org.snmp4j.mp.MutableStateReference;
import org.snmp4j.mp.PduHandle;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.mp.StateReference;
import org.snmp4j.mp.StatusInformation;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

/**
 * SNMP response handler
 */
public class SnmpDecoder extends SimpleChannelInboundHandler<DatagramPacket> {

  private static final Logger LOG = LoggerFactory.getLogger(SnmpDecoder.class);

  private static final List<MessageProcessingModel> SUPPORTED_MODELS = new ArrayList<MessageProcessingModel>(Arrays.asList(new MPv1(), new MPv2c(), new MPv3()));

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) throws Exception {

    InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
    SocketAddress remoteAddress = ctx.channel().remoteAddress();

    if (msg == null || msg.content() == null || msg.content().readableBytes() <= 0) {
      LOG.trace("Empty DatagramPacket. ignoring");
      return;
    }

    // @see  MessageDispatcherImpl.dispatchMessage

    BERInputStream wholeMessage = new BERInputStream(msg.content().nioBuffer());
    PDU pdu = null;
    int status = Integer.MIN_VALUE;
    try {
      LOG.trace("Extracting PDU from DatagramPacket...");
      MutablePDU mutablePdu = new MutablePDU();
      Integer32 messageProcessingModel = new Integer32();
      Integer32 securityModel = new Integer32();
      OctetString securityName = new OctetString();
      Integer32 securityLevel = new Integer32();

      PduHandle handle = new PduHandle(0);

      Integer32 maxSizeRespPDU = new Integer32(65535);
      StatusInformation statusInfo = new StatusInformation();
      MutableStateReference<Address> mutableStateReference = new MutableStateReference<>();
      // add the transport mapping to the state reference to allow the MP to
      // return REPORTs on the same interface/port the message had been received.
      StateReference<Address> stateReference = new StateReference<>();
      UdpAddress localUdpAddress = new UdpAddress(localAddress.getAddress(), localAddress.getPort());
      stateReference.setAddress(localUdpAddress);
      mutableStateReference.setStateReference(stateReference);

      TransportStateReference tsr = new TransportStateReference(null, localUdpAddress, (OctetString) null, SecurityLevel.undefined, SecurityLevel.undefined, false, ctx.channel());

      wholeMessage.mark(16);
      BER.MutableByte type = new BER.MutableByte();
      // decode header but do not check length here, because we do only decode
      // the first 16 bytes.
      BER.decodeHeader(wholeMessage, type, false);
      if (type.getValue() != BER.SEQUENCE) {
        LOG.trace("ASN.1 parse error (message is not a sequence)");
      }
      Integer32 version = new Integer32();
      version.decodeBER(wholeMessage);
      MessageProcessingModel mp = getMessageProcessingModel(version.getValue());

      if (mp == null) {
        LOG.debug("MessageProcessingModel not found for id: {}", version);
      }

      wholeMessage.reset();

      status = mp.prepareDataElements(
          null, //messageDispatcher -  if reply is needed it will be made by  the next handlers 
          localUdpAddress,
          wholeMessage,
          tsr,
          messageProcessingModel,
          securityModel,
          securityName,
          securityLevel,
          mutablePdu,
          handle,
          maxSizeRespPDU,
          statusInfo,
          mutableStateReference);

      pdu = mutablePdu.getPdu();
    } catch (Exception e) {
      LOG.trace("Exception in processing.  Packet received from {} on {}: {}", remoteAddress, localAddress, new OctetString(wholeMessage.getBuffer().array()).toHexString(), e);
    }

    if (status == SnmpConstants.SNMP_ERROR_SUCCESS && pdu != null) {
      int pduType = pdu.getType();
      if ((pduType != PDU.TRAP) && (pduType != PDU.V1TRAP) && (pduType != PDU.REPORT) && (pduType != PDU.RESPONSE)) {
        LOG.trace("not a TRAP or RESPONSE ignoring: {}", pdu.toString());
      }

      ctx.fireChannelRead(pdu);
    } else {
      LOG.trace("ProcessingStatus failed ({}). Packet received from {} on {}: {}", status, remoteAddress, localAddress, new OctetString(wholeMessage.getBuffer().array()).toHexString());
    }
  }

  private MessageProcessingModel getMessageProcessingModel(int mId) {
    try {
      return SUPPORTED_MODELS.get(mId);
    } catch (IndexOutOfBoundsException iobex) {
      return null;
    }
  }
}