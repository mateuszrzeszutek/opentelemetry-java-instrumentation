/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapSetter;

final class ClientInstrumenter<REQUEST, RESPONSE> extends Instrumenter<REQUEST, RESPONSE> {

  private final ContextPropagators propagators;
  private final TextMapSetter<REQUEST> setter;

  ClientInstrumenter(
      Tracer tracer,
      SpanNameExtractor<? super REQUEST> spanNameExtractor,
      SpanKind spanKind,
      StatusExtractor<? super REQUEST, ? super RESPONSE> statusExtractor,
      Iterable<? extends AttributesExtractor<? super REQUEST, ? super RESPONSE>>
          attributesExtractors,
      ErrorCauseExtractor errorCauseExtractor,
      ContextPropagators propagators,
      TextMapSetter<REQUEST> setter) {
    super(
        tracer,
        spanNameExtractor,
        spanKind,
        statusExtractor,
        attributesExtractors,
        errorCauseExtractor);
    this.propagators = propagators;
    this.setter = setter;
  }

  @Override
  public Context start(Context parentContext, REQUEST request) {
    Context newContext = super.start(parentContext, request);
    propagators.getTextMapPropagator().inject(newContext, request, setter);
    return newContext;
  }
}
