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

import io.vertx.core.Completable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpProtocol;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.internal.resolver.NameResolver;
import io.vertx.core.net.Address;
import io.vertx.core.net.ClientSSLOptions;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.impl.SocketAddressImpl;
import io.vertx.core.spi.endpoint.EndpointBuilder;
import io.vertx.core.spi.endpoint.EndpointResolver;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A resolver for origins.
 *
 * <p>This class resolves {@link Origin} address to {@link OriginEndpoint}, an endpoint maintains
 *
 * <ul>
 *   <li>a primary {@link OriginServer} obtained from DNS resolution and for which we cannot really make an assumption on
 *  the HTTP protocol it can handle (although it is TCP based).</li>
 *  <li>a list of {@link OriginServer} alternatives for which the HTTP protocol is determined by the alternative services discovery</li>
 * </ul>
 *
 * <p>The initial resolution uses the classic A/AAAA DNS resolution and thus initially an origin endpoint
 * is the list of DNS resolved socket addresses.</p>
 *
 * <p>When the client considers a connection could send alt-svc events, it sets an alt-svc event handler on the
 * connection and relays those events to the origin resolver: an origin endpoint is looked up and when the endpoint is
 * found, it is updated with the alternative services and potentially considered stale.</p>
 *
 * <p>When an origin endpoint is stale, the resolver updates it on the next resolver lookup, the DNS resolver is then
 * used to obtain the alternative services addresses and keep only the ones we can DNS resolve.</p>
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class OriginResolver<L> implements EndpointResolver<Origin, OriginServer, OriginEndpoint<L>, L> {

  private final VertxInternal vertx;
  private final HttpClientImpl client;
  private final ConcurrentMap<Origin, OriginEndpoint<L>> endpoints;
  private final boolean resolveAll;

  public OriginResolver(VertxInternal vertx, boolean resolveAll, HttpClientImpl client) {
    this.vertx = vertx;
    this.endpoints = new ConcurrentHashMap<>();
    this.resolveAll = resolveAll;
    this.client = client;
  }

  public void clearAlternatives(Origin origin) {
    OriginEndpoint<L> endpoint = endpoints.get(origin);
    if (endpoint != null) {
      endpoint.updateAlternatives(Collections.emptyMap());
    }
  }

  public void updateAlternatives(ClientSSLOptions sslOptions,  Origin origin, AltSvc.ListOfValue altSvc) {
    OriginEndpoint<L> endpoint = endpoints.get(origin);
    if (endpoint != null) {
      Map<OriginAlternative, Long> updates = endpoint.shouldRefresh(altSvc);
      if (updates != null) {
        Future<Map<OriginAlternative, OriginServer>> result = compute(sslOptions, origin, endpoint.primary, updates);
        result.onComplete(ar -> {
          if (ar.succeeded()) {
            endpoint.updateAlternatives(ar.result());
          }
        });
      }
    }
  }

  private Future<Map<OriginAlternative, OriginServer>> compute(
    ClientSSLOptions sslOptions,
    Origin address,
    OriginServer primary,
    Map<OriginAlternative, Long> updates) {

    class Resolution extends LinkedHashMap<OriginAlternative, Long> {
      final String host;
      public Resolution(String host) {
        this.host = host;
      }
    }

    // Keep order
    Map<OriginAlternative, OriginServer> alternatives = new LinkedHashMap<>();
    Map<String, Resolution> hosts = new HashMap<>();
    for (Map.Entry<OriginAlternative, Long> entry : updates.entrySet()) {
      // Already resolved
      OriginAlternative alternative = entry.getKey();
      long maxAge = entry.getValue();
      if (alternative.authority.host().equals(address.host)) {
        alternative = new OriginAlternative(
          alternative.protocol,
          HostAndPort.authority(address.host, alternative.authority.port()));
        alternatives.put(alternative, new OriginServer(
          false,
          alternative.protocol,
          alternative.authority,
          SocketAddress.inetSocketAddress(new InetSocketAddress(((SocketAddressImpl)primary.address).ipAddress(), alternative.authority.port())),
          maxAge));
      } else {
        Resolution resolution = hosts.get(alternative.authority.host());
        if (resolution == null) {
          resolution = new Resolution(alternative.authority.host());
          hosts.put(alternative.authority.host(), resolution);
        }
        resolution.put(alternative, maxAge);
      }
    }

    List<Resolution> resolutions = new ArrayList<>(hosts.values());
    int size = hosts.size();
    List<Future<InetAddress>> list = new ArrayList<>(size);
    for (Resolution resolution : resolutions) {
      Future<InetAddress> fut = vertx
        .nameResolver()
        .resolve(resolution.host);
      list.add(fut);
    }

    Promise<Void> promise = Promise.promise();

    if (size > 0) {
      AtomicInteger count = new AtomicInteger();
      Completable<InetAddress> joiner = (result, failure) -> {
        if (count.incrementAndGet() == size) {
          for (int i = 0; i < size; i++) {
            Resolution r = resolutions.get(i);
            Future<InetAddress> f = list.get(i);
            if (f.succeeded()) {
              for (Map.Entry<OriginAlternative, Long> entry : r.entrySet()) {
                OriginAlternative alternative = entry.getKey();
                long maxAge = entry.getValue();
                alternatives.put(alternative,
                  new OriginServer(
                    false,
                    alternative.protocol,
                    alternative.authority,
                    SocketAddress.inetSocketAddress(new InetSocketAddress(f.result(), alternative.authority.port())),
                    maxAge)
                );
              }
            }
          }
          promise.succeed();
        }
      };
      for (Future<InetAddress> f : list) {
        f.onComplete(joiner);
      }
    } else {
      promise.succeed();
    }

    Promise<Map<OriginAlternative, OriginServer>> p2 = Promise.promise();
    promise
      .future()
      .onComplete(ar -> {
        if (ar.succeeded()) {


          List<Future<?>> l = new ArrayList<>();
          for (Map.Entry<OriginAlternative, OriginServer> e : alternatives.entrySet()) {
            Future<?> f = client.checkConnect(primary, e.getKey(), e.getValue(), sslOptions);
            l.add(f);
          }

          CompositeFuture cf = Future.join(l);
          cf.onComplete(ar2 -> {

            Iterator<Map.Entry<OriginAlternative, OriginServer>> it = alternatives.entrySet().iterator();
            Map<OriginAlternative, OriginServer> newOne = new LinkedHashMap<>();
            int idx = 0;
            while (it.hasNext()) {
              Map.Entry<OriginAlternative, OriginServer> entry = it.next();
              newOne.put(entry.getKey(), entry.getValue());
              entry.getValue().available = cf.succeeded(idx++);
            }
            p2.complete(newOne);
          });

        } else {
          p2.fail(ar.cause());
        }
      });

    // Now validate

    return p2.future();
  }

  @Override
  public Origin tryCast(Address address) {
    return address instanceof Origin ? (Origin)address : null;
  }

  @Override
  public SocketAddress addressOf(OriginServer server) {
    return server.address != null ? server.address : null;
  }

  @Override
  public String protocolOf(OriginServer server) {
    HttpProtocol protocol = server.protocol;
    return protocol != null ? protocol.id() : null;
  }

  @Override
  public Future<OriginEndpoint<L>> resolve(Origin address, EndpointBuilder<L, OriginServer> builder) {
    NameResolver resolver = vertx
      .nameResolver();
    HostAndPort authority = HostAndPort.authority(address.host, address.port);
    if (resolveAll) {
      return resolver
        .resolveAll(address.host)
        .map(res -> {
          List<OriginServer> primary = new ArrayList<>(res.size());
          for (InetSocketAddress addr : res) {
            primary.add(new OriginServer(true, null, authority, SocketAddress.inetSocketAddress(new InetSocketAddress(addr.getAddress(), address.port)), Long.MAX_VALUE));
          }
          OriginEndpoint<L> endpoint = new OriginEndpoint<>(address, primary, builder, Collections.emptyMap());
          endpoints.put(address, endpoint);
          return endpoint;
        });
    } else {
      return resolver
        .resolve(address.host)
        .map(addr -> {
          OriginServer primary = new OriginServer(true,null, authority, SocketAddress.inetSocketAddress(new InetSocketAddress(addr, address.port)), Long.MAX_VALUE);
          OriginEndpoint<L> endpoint = new OriginEndpoint<>(address, primary, builder, Collections.emptyMap());
          endpoints.put(address, endpoint);
          return endpoint;
        });
    }
  }

  @Override
  public L endpoint(OriginEndpoint<L> state) {
    return state.list;
  }

  @Override
  public boolean isValid(OriginEndpoint<L> state) {
    return state.validate();
  }

  @Override
  public Future<OriginEndpoint<L>> refresh(Origin address, OriginEndpoint<L> state) {
    OriginEndpoint<L> endpoint = new OriginEndpoint<>(address, state.primary, state.builder, state.updates);
    endpoints.put(address, endpoint);
    return Future.succeededFuture(endpoint);
  }

  @Override
  public void dispose(OriginEndpoint<L> data) {
    endpoints.remove(data.origin, data);
  }

  @Override
  public void close() {
  }
}
