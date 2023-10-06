/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.pinot.client.utils.ConnectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Pinot client transport factory for JSON encoded BrokerResults through HTTP.
 */
public class JsonAsyncHttpPinotClientTransportFactory implements PinotClientTransportFactory {

  private static final String DEFAULT_BROKER_READ_TIMEOUT_MS = "60000";
  private static final String DEFAULT_BROKER_CONNECT_TIMEOUT_MS = "2000";
  private static final String DEFAULT_BROKER_HANDSHAKE_TIMEOUT_MS = "2000";
  private static final String DEFAULT_BROKER_TLS_V10_ENABLED = "false";

  @Getter
  @Setter
  private Map<String, String> _headers = new HashMap<>();
  @Getter
  @Setter
  private String _scheme = CommonConstants.HTTP_PROTOCOL;
  @Getter
  @Setter
  private SSLContext _sslContext = null;
  private boolean _tlsV10Enabled = false;
  private int _readTimeoutMs = Integer.parseInt(DEFAULT_BROKER_READ_TIMEOUT_MS);
  private int _connectTimeoutMs = Integer.parseInt(DEFAULT_BROKER_READ_TIMEOUT_MS);
  private int _handshakeTimeoutMs = Integer.parseInt(DEFAULT_BROKER_HANDSHAKE_TIMEOUT_MS);
  private String _appId = null;
  private String _extraOptionString;
  private boolean _useMultiStageEngine;

  @Override
  public PinotClientTransport buildTransport() {
    ConnectionTimeouts connectionTimeouts =
        ConnectionTimeouts.create(_readTimeoutMs, _connectTimeoutMs, _handshakeTimeoutMs);
    TlsProtocols tlsProtocols = TlsProtocols.defaultProtocols(_tlsV10Enabled);
    return new JsonAsyncHttpPinotClientTransport(_headers, _scheme, _extraOptionString, _useMultiStageEngine,
        _sslContext, connectionTimeouts, tlsProtocols, _appId);
  }

  public JsonAsyncHttpPinotClientTransportFactory withConnectionProperties(Properties properties) {
    if (_headers == null || _headers.isEmpty()) {
      _headers = ConnectionUtils.getHeadersFromProperties(properties);
    }

    if (_scheme == null) {
      _scheme = properties.getProperty("scheme", CommonConstants.HTTP_PROTOCOL);
    }

    if (_sslContext == null && _scheme.contentEquals(CommonConstants.HTTPS_PROTOCOL)) {
      _sslContext = ConnectionUtils.getSSLContextFromProperties(properties);
    }

    _readTimeoutMs = Integer.parseInt(properties.getProperty("brokerReadTimeoutMs", DEFAULT_BROKER_READ_TIMEOUT_MS));
    _connectTimeoutMs =
        Integer.parseInt(properties.getProperty("brokerConnectTimeoutMs", DEFAULT_BROKER_CONNECT_TIMEOUT_MS));
    _handshakeTimeoutMs =
        Integer.parseInt(properties.getProperty("brokerHandshakeTimeoutMs", DEFAULT_BROKER_HANDSHAKE_TIMEOUT_MS));
    _appId = properties.getProperty("appId");
    _tlsV10Enabled = Boolean.parseBoolean(properties.getProperty("brokerTlsV10Enabled", DEFAULT_BROKER_TLS_V10_ENABLED))
        || Boolean.parseBoolean(
        System.getProperties().getProperty("broker.tlsV10Enabled", DEFAULT_BROKER_TLS_V10_ENABLED));

    _extraOptionString = properties.getProperty("queryOptions", "");
    _useMultiStageEngine = Boolean.parseBoolean(properties.getProperty("useMultiStageEngine", "false"));
    return this;
  }
}
