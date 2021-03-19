/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.instrumentation.api.tracer.AttributeSetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class NetAttributesExtractor<REQUEST, RESPONSE>
    extends AttributesExtractor<REQUEST, RESPONSE> {

  @Override
  final void onStart(AttributeSetter setter, REQUEST request) {
    set(setter, SemanticAttributes.NET_TRANSPORT, transport(request));
  }

  @Override
  final void onEnd(AttributeSetter setter, REQUEST request, RESPONSE response) {
    set(setter, SemanticAttributes.NET_PEER_IP, peerIp(request, response));

    // TODO(anuraaga): Clients don't have peer information available during the request usually.
    // By only resolving them after the response, we can simplify the code a lot but sacrifice
    // having them available during sampling on the server side. Revisit if that seems important.
    set(setter, SemanticAttributes.NET_PEER_NAME, peerName(request, response));
    set(setter, SemanticAttributes.NET_PEER_PORT, peerPort(request, response));
  }

  @Nullable
  protected abstract String transport(REQUEST request);

  @Nullable
  protected abstract String peerName(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract Long peerPort(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract String peerIp(REQUEST request, RESPONSE response);
}
