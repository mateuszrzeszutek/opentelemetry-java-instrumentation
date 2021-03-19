/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;

final class ServerInstrumenter<REQUEST, RESPONSE> extends Instrumenter<REQUEST, RESPONSE> {

  private final ContextPropagators propagators;
  private final TextMapGetter<REQUEST> getter;

  ServerInstrumenter(
      Tracer tracer,
      SpanNameExtractor<? super REQUEST> spanNameExtractor,
      SpanKind spanKind,
      StatusExtractor<? super REQUEST, ? super RESPONSE> statusExtractor,
      Iterable<? extends AttributesExtractor<? super REQUEST, ? super RESPONSE>>
          attributesExtractors,
      ErrorCauseExtractor errorCauseExtractor,
      ContextPropagators propagators,
      TextMapGetter<REQUEST> getter) {
    super(
        tracer,
        spanNameExtractor,
        spanKind,
        statusExtractor,
        attributesExtractors,
        errorCauseExtractor);
    this.propagators = propagators;
    this.getter = getter;
  }

  @Override
  public Context start(Context parentContext, REQUEST request) {
    Context extracted = propagators.getTextMapPropagator().extract(parentContext, request, getter);
    return super.start(extracted, request);
  }
}
