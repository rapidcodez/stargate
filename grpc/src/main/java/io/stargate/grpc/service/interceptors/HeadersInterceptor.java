package io.stargate.grpc.service.interceptors;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.stargate.grpc.service.Service;
import java.util.HashMap;
import java.util.Map;

public class HeadersInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Map<String, String> stringHeaders = new HashMap<>();
    for (String key : headers.keys()) {
      if (key.endsWith("-bin")) {
        continue;
      }
      String value = headers.get(Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
      stringHeaders.put(key, value);
    }
    Context context = Context.current();
    context = context.withValue(Service.HEADERS_KEY, stringHeaders);
    return Contexts.interceptCall(context, call, headers, next);
  }
}
