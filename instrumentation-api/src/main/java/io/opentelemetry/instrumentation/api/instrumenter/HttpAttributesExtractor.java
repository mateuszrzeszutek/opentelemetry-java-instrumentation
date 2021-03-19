/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.instrumentation.api.tracer.AttributeSetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class HttpAttributesExtractor<REQUEST, RESPONSE>
    extends AttributesExtractor<REQUEST, RESPONSE> {

  @Override
  final void onStart(AttributeSetter setter, REQUEST request) {
    set(setter, SemanticAttributes.HTTP_METHOD, method(request));
    set(setter, SemanticAttributes.HTTP_URL, url(request));
    set(setter, SemanticAttributes.HTTP_TARGET, target(request));
    set(setter, SemanticAttributes.HTTP_HOST, host(request));
    set(setter, SemanticAttributes.HTTP_ROUTE, route(request));
    set(setter, SemanticAttributes.HTTP_SCHEME, scheme(request));
    set(setter, SemanticAttributes.HTTP_USER_AGENT, userAgent(request));
  }

  @Override
  final void onEnd(AttributeSetter setter, REQUEST request, RESPONSE response) {
    set(
        setter,
        SemanticAttributes.HTTP_REQUEST_CONTENT_LENGTH,
        requestContentLength(request, response));
    set(
        setter,
        SemanticAttributes.HTTP_REQUEST_CONTENT_LENGTH_UNCOMPRESSED,
        requestContentLengthUncompressed(request, response));
    set(setter, SemanticAttributes.HTTP_STATUS_CODE, statusCode(request, response));
    set(setter, SemanticAttributes.HTTP_FLAVOR, flavor(request, response));
    set(
        setter,
        SemanticAttributes.HTTP_RESPONSE_CONTENT_LENGTH,
        responseContentLength(request, response));
    set(
        setter,
        SemanticAttributes.HTTP_RESPONSE_CONTENT_LENGTH_UNCOMPRESSED,
        responseContentLengthUncompressed(request, response));
    set(setter, SemanticAttributes.HTTP_SERVER_NAME, serverName(request, response));
    set(setter, SemanticAttributes.HTTP_CLIENT_IP, clientIp(request, response));
  }

  // Attributes that always exist in a request

  @Nullable
  protected abstract String method(REQUEST request);

  @Nullable
  protected abstract String url(REQUEST request);

  @Nullable
  protected abstract String target(REQUEST request);

  @Nullable
  protected abstract String host(REQUEST request);

  @Nullable
  protected abstract String route(REQUEST request);

  @Nullable
  protected abstract String scheme(REQUEST request);

  @Nullable
  protected abstract String userAgent(REQUEST request);

  // Attributes which are not always available when the request is ready.

  @Nullable
  protected abstract Long requestContentLength(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract Long requestContentLengthUncompressed(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract Long statusCode(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract String flavor(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract Long responseContentLength(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract Long responseContentLengthUncompressed(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract String serverName(REQUEST request, RESPONSE response);

  @Nullable
  protected abstract String clientIp(REQUEST request, RESPONSE response);
}
