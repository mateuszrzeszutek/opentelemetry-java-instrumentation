/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.tracer.ClientSpan;
import io.opentelemetry.instrumentation.api.tracer.ServerSpan;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public /* sealed */ class Instrumenter<REQUEST, RESPONSE> {

  public static <REQUEST, RESPONSE> InstrumenterBuilder<REQUEST, RESPONSE> newBuilder(
      OpenTelemetry openTelemetry, String instrumentationName) {
    return new InstrumenterBuilder<>(openTelemetry, instrumentationName);
  }

  private final Tracer tracer;
  private final SpanNameExtractor<? super REQUEST> spanNameExtractor;
  private final SpanKind spanKind;
  private final StatusExtractor<? super REQUEST, ? super RESPONSE> statusExtractor;
  private final List<AttributesExtractor<? super REQUEST, ? super RESPONSE>> extractors;
  private final ErrorCauseExtractor errorCauseExtractor;

  Instrumenter(
      Tracer tracer,
      SpanNameExtractor<? super REQUEST> spanNameExtractor,
      SpanKind spanKind,
      StatusExtractor<? super REQUEST, ? super RESPONSE> statusExtractor,
      Iterable<? extends AttributesExtractor<? super REQUEST, ? super RESPONSE>> extractors,
      ErrorCauseExtractor errorCauseExtractor) {
    this.tracer = tracer;
    this.spanNameExtractor = spanNameExtractor;
    this.spanKind = spanKind;
    this.statusExtractor = statusExtractor;
    this.extractors =
        StreamSupport.stream(extractors.spliterator(), false).collect(Collectors.toList());
    this.errorCauseExtractor = errorCauseExtractor;
  }

  public Context start(Context parentContext, REQUEST request) {
    SpanBuilder spanBuilder =
        tracer
            .spanBuilder(spanNameExtractor.extract(request))
            .setSpanKind(spanKind)
            .setParent(parentContext);

    for (AttributesExtractor<? super REQUEST, ? super RESPONSE> extractor : extractors) {
      extractor.onStart(spanBuilder::setAttribute, request);
    }

    Span span = spanBuilder.startSpan();
    Context context = parentContext.with(span);
    switch (spanKind) {
      case SERVER:
        return ServerSpan.with(context, span);
      case CLIENT:
        return ClientSpan.with(context, span);
      default:
        return context;
    }
  }

  public void end(Context context, REQUEST request, RESPONSE response, Throwable error) {
    Span span = Span.fromContext(context);

    for (AttributesExtractor<? super REQUEST, ? super RESPONSE> extractor : extractors) {
      extractor.onEnd(span::setAttribute, request, response);
    }

    if (error != null) {
      error = errorCauseExtractor.extractCause(error);
      span.recordException(error);
    }

    span.setStatus(statusExtractor.extract(request, response, error));

    span.end();
  }
}
