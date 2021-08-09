/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc.impl;

import io.grpc.Server;
import io.grpc.internal.GrpcUtil;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.grpc.service.Service;
import io.stargate.grpc.service.interceptors.AuthenticationInterceptor;
import io.stargate.grpc.service.interceptors.RemoteAddressInterceptor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcImpl {
  private static final Logger logger = LoggerFactory.getLogger(GrpcImpl.class);
  private static final Integer EXECUTOR_SIZE =
      Integer.getInteger(
          "stargate.grpc.executor_size", Runtime.getRuntime().availableProcessors() * 2);

  private final Server server;
  private final ScheduledExecutorService executor;

  public GrpcImpl(
      Persistence persistence, Metrics metrics, AuthenticationService authenticationService) {

    String listenAddress = null;

    try {
      listenAddress =
          System.getProperty(
              "stargate.listen_address", InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }

    if (!Boolean.getBoolean("stargate.bind_to_listen_address")) listenAddress = "0.0.0.0";

    int port = Integer.getInteger("stargate.grpc.port", 8090);

    executor =
        Executors.newScheduledThreadPool(
            EXECUTOR_SIZE, GrpcUtil.getThreadFactory("grpc-stargate-executor", true));
    server =
        NettyServerBuilder.forAddress(new InetSocketAddress(listenAddress, port))
            .executor(executor)
            .intercept(new AuthenticationInterceptor(authenticationService))
            .intercept(new RemoteAddressInterceptor())
            .addService(new Service(persistence, metrics, executor))
            .build();
  }

  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void stop() {
    try {
      server.shutdown().awaitTermination();
      // Since we provided our own executor, it's our responsibility to shut it down.
      // Note that we don't handle restarts because GrpcActivator never reuses an existing instance
      // (and that wouldn't work anyway, because Server doesn't support it either).
      executor.shutdown();
      if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        logger.warn("Timed out while waiting for executor shutdown");
      }
    } catch (InterruptedException e) {
      logger.error("Failed waiting for gRPC shutdown", e);
    }
  }
}
