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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.snmp4j.PDU;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.SMIConstants;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;

import net.percederberg.mibble.Mib;
import net.percederberg.mibble.MibLoader;
import net.percederberg.mibble.MibValueSymbol;
import net.percederberg.mibble.snmp.SnmpType;
import net.percederberg.mibble.value.ObjectIdentifierValue;

public class PDUConverter implements Configurable {

  public final class Names {
    public static final String FIELD_VARS = "variables";
    public static final String FIELD_REQUEST_ID = "requestId";
    public static final String FIELD_OID = "oid";
    public static final String FIELD_TYPE = "type";

    public static final String FIELD_MIB_DESCR = "description";
    public static final String FIELD_MIB_NAME = "name";

    public static final String SMI_COUNTER32 = "counter32";
    public static final String SMI_COUNTER64 = "counter64";
    public static final String SMI_GAUGE32 = "gauge32";
    public static final String SMI_INTEGER = "integer";
    public static final String SMI_IPADDRESS = "ipaddress";
    public static final String SMI_NULL = "null";
    public static final String SMI_OBJECTIDENTIFIER = "objectIdentifier";
    public static final String SMI_OCTETSTRING = "octetString";
    public static final String SMI_OPAQUE = "opaque";
    public static final String SMI_TIMETICKS = "timeticks";
  }

  public static final String MIB_ENABLED_CONFIG = "mib.enabled";
  private static final Boolean MIB_ENABLED_DEFAULT = Boolean.FALSE;

  public static final String MIB_RESOURCES_CONFIG = "mib.resources";
  private static final List<String> MIB_RESOURCES_DEFAULT = Collections.emptyList();

  public static final String MIB_NAMES_CONFIG = "mib.names";
  private static final List<String> MIB_NAMES_DEFAULT = Collections.emptyList();

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(MIB_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, MIB_ENABLED_DEFAULT, ConfigDef.Importance.MEDIUM, "Enable MIB")
      .define(MIB_RESOURCES_CONFIG, ConfigDef.Type.LIST, MIB_RESOURCES_DEFAULT, ConfigDef.Importance.MEDIUM, "MIB resources")
      .define(MIB_NAMES_CONFIG, ConfigDef.Type.LIST, MIB_NAMES_DEFAULT, ConfigDef.Importance.MEDIUM, "MIB names");

  private ObjectIdentifierValue rootOid;
  private List<Mib> mibs;
  private Boolean mibEnabled;

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    this.mibEnabled = conf.getBoolean(MIB_ENABLED_CONFIG);

    List<String> resources = conf.getList(MIB_RESOURCES_CONFIG);
    List<String> mibNames = conf.getList(MIB_NAMES_CONFIG);

    if (this.mibEnabled) {
      try {
        MibLoader loader = new MibLoader();
        for (String res : resources) {
          loader.addResourceDir(res);
        }
        
        this.mibs = new ArrayList<>(mibNames.size());
        for (String mibName : mibNames) {
          mibs.add(loader.load(mibName));
        }
        this.rootOid = loader.getRootOid();
      } catch (Exception e) {
        throw new ConnectException("Failed to initialize MIB", e);
      }
    }

  }

  public Map<String, Object> convert(PDU pdu) {

    Map<String, Object> value = new HashMap<>();

    VariableBinding[] pduContents = pdu.toArray();
    Integer32 requestId = pdu.getRequestID();
    value.put(Names.FIELD_REQUEST_ID, requestId.toString());

    if (null != pduContents && pduContents.length > 0) {
      List<Map<String, Object>> bindingStructs = new ArrayList<>(pduContents.length);
      for (VariableBinding binding : pduContents) {
        Map<String, Object> bindingStruct = convertVariableBinding(binding);
        bindingStructs.add(bindingStruct);
      }

      value.put(Names.FIELD_VARS, bindingStructs);
    }
    return value;
  }

  private Map<String, Object> convertVariableBinding(VariableBinding binding) {

    Map<String, Object> varData = new HashMap<>();
    final String oid = binding.getOid().toDottedString();
    varData.put(Names.FIELD_OID, oid);

    if (this.mibEnabled) {
      varData.putAll(getMibData(oid));
    }

    final Variable variable = binding.getVariable();
    final String syntaxType;
    final Object value;

    switch (binding.getSyntax()) {
      case SMIConstants.SYNTAX_COUNTER32:
        syntaxType = Names.SMI_COUNTER32;
        value = variable.toInt();
        break;
      case SMIConstants.SYNTAX_COUNTER64:
        syntaxType = Names.SMI_COUNTER64;
        value = variable.toLong();
        break;
      case SMIConstants.SYNTAX_GAUGE32:
        syntaxType = Names.SMI_GAUGE32;
        value = variable.toInt();
        break;
      case SMIConstants.SYNTAX_INTEGER:
        syntaxType = Names.SMI_INTEGER;
        value = variable.toInt();
        break;
      case SMIConstants.SYNTAX_IPADDRESS:
        syntaxType = Names.SMI_IPADDRESS;
        value = variable.toString();
        break;
      case SMIConstants.SYNTAX_NULL:
        syntaxType = Names.SMI_NULL;
        value = null;
        break;
      case SMIConstants.SYNTAX_OBJECT_IDENTIFIER:
        syntaxType = Names.SMI_OBJECTIDENTIFIER;
        if (this.mibEnabled) {
          Map<String, Object> oidData = getMibData(variable.toString());
          oidData.put(Names.FIELD_OID, variable.toString());
          value = oidData;
        } else {
          value = variable.toString();
        }
        break;
      case SMIConstants.SYNTAX_OCTET_STRING:
        syntaxType = Names.SMI_OCTETSTRING;
        value = variable.toString();
        break;
      case SMIConstants.SYNTAX_OPAQUE:
        syntaxType = Names.SMI_OPAQUE;
        value = variable.toString();
        break;
      case SMIConstants.SYNTAX_TIMETICKS:
        syntaxType = Names.SMI_TIMETICKS;
        value = variable.toInt();
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("%s is an unsupported syntaxType.", binding.getSyntax()));
    }

    varData.put(Names.FIELD_TYPE, syntaxType);
    varData.put("value", value);
    return varData;
  }

  private Map<String, Object> getMibData(String oid) {

    MibValueSymbol foundSymbol = null;
    for (Mib mib : mibs) {
      MibValueSymbol mibSymbol = mib.getSymbolByOid(oid);
      if (mibSymbol != null && oid.equals(mibSymbol.getOid().toString())) {
        foundSymbol = mibSymbol;
      }
      if (foundSymbol != null) {
        break;
      }
    }

    if (foundSymbol == null) {
      //try from root 
      ObjectIdentifierValue foundValue = rootOid.find(oid);
      if (foundValue != null && oid.equals(foundValue.getSymbol().getOid().toString())) {
        foundSymbol = foundValue.getSymbol();
      }
    }

    Map<String, Object> mibData = new HashMap<>();
    if (foundSymbol != null) {
      mibData.put(Names.FIELD_OID, foundSymbol.getOid().toString());
      mibData.put(Names.FIELD_MIB_NAME, foundSymbol.getName());

      if (foundSymbol.getType() instanceof SnmpType) {
        mibData.put(Names.FIELD_MIB_DESCR, ((SnmpType) foundSymbol.getType()).getDescription());
      }
    } else {
      mibData.put(Names.FIELD_OID, oid);
    }

    return mibData;

  }

}