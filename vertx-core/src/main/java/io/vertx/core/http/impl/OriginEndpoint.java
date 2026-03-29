/*
 * Copyright (c) 2011-2025 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.http.impl;

import io.vertx.core.http.HttpProtocol;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.spi.endpoint.EndpointBuilder;

import java.util.*;

/**
 * An endpoint for a given origin
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class OriginEndpoint<L> {

  final long timestamp;
  final Origin origin;
  final OriginServer primary;
  final List<OriginServer> primaries;
  final EndpointBuilder<L, OriginServer> builder;
  final L list;
  private final Map<OriginAlternative, OriginServer> alternatives;

  Map<OriginAlternative, OriginServer> updates;
  volatile boolean valid;

  OriginEndpoint(Origin origin, OriginServer primary, EndpointBuilder<L, OriginServer> builder, Map<OriginAlternative, OriginServer> alternatives) {
    this(origin, List.of(primary), builder, alternatives);
  }

  OriginEndpoint(Origin origin, List<OriginServer> primaries, EndpointBuilder<L, OriginServer> builder, Map<OriginAlternative, OriginServer> alternatives) {

    L list = buildListOfServers(builder, primaries, alternatives);

    this.timestamp = System.currentTimeMillis();
    this.primary = primaries.get(0);
    this.primaries = primaries;
    this.origin = origin;
    this.builder = builder;
    this.alternatives = alternatives;
    this.list = list;
    this.updates = Collections.emptyMap();
    this.valid = true;
  }

  private L buildListOfServers(EndpointBuilder<L, OriginServer> builder, List<OriginServer> primaries, Map<OriginAlternative, OriginServer> alternatives) {
    for (OriginServer primary : primaries) {
      if (primary.available) {
        builder = builder.addServer(primary);
      }
    }
    for (OriginServer alternativeServer : alternatives.values()) {
      if (alternativeServer.available) {
        builder.addServer(alternativeServer);
      }
    }
    return builder.build();
  }

  boolean validate() {
    if (valid) {
      long now = System.currentTimeMillis();
      for (OriginServer alternative : alternatives.values()) {
        if (now >= timestamp + alternative.maxAge * 1000) {
          valid = false;
          return false;
        }
      }
    }
    return valid;
  }

  void updateAlternatives(Map<OriginAlternative, OriginServer> updates) {
    this.updates = updates;
    this.valid = false;
  }

  Map<OriginAlternative, Long> shouldRefresh(AltSvc.ListOfValue altSvc) {
    long now = System.currentTimeMillis();
    Map<OriginAlternative, Long> updates = new LinkedHashMap<>();
    boolean valid = true;
    for (AltSvc.Value altSvcValue : altSvc) {
      HttpProtocol protocol = HttpProtocol.fromId(altSvcValue.protocolId());
      // We only care about those protocols
      if (protocol == HttpProtocol.HTTP_1_1 || protocol == HttpProtocol.H2 || protocol == HttpProtocol.H3) {
        long maxAge;
        String ma = altSvcValue.parameters().get("ma");
        if (ma != null) {
          try {
            maxAge = Long.parseLong(ma);
          } catch (NumberFormatException ex) {
            continue;
          }
        } else {
          maxAge = 24 * 3600;
        }
        HostAndPort altAuthority = altSvcValue.altAuthority();
        if (altAuthority.host().isEmpty()) {
          altAuthority = HostAndPort.create(primary.authority.host(), altAuthority.port());
        }
        OriginAlternative alternative = new OriginAlternative(protocol, altAuthority);
        valid &= alternatives.containsKey(alternative);
        if (valid) {
          // We consider alternative is still fresh when the expiration timestamp computed
          // with the new max age divided by two is older than the current expiration timestamp
          // this caches the alternative entry
          long alternativeCachedExpiration = timestamp + alternatives.get(alternative).maxAge * 1000;
          long value = now + maxAge * 1000 / 2;
          valid = (value < alternativeCachedExpiration);
        }
        updates.put(alternative, maxAge);
      }
    }
    if (valid) {
      // 1. check now we don't have extra unwanted keys
      for (OriginAlternative alternative : alternatives.keySet()) {
        valid &= updates.containsKey(alternative);
      }
    }
    return valid ? null : updates;
  }
}
