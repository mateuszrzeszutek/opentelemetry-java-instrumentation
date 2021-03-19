/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.instrumentation.api.InstrumentationVersion;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InstrumenterBuilder<REQUEST, RESPONSE> {
  private final OpenTelemetry openTelemetry;
  private final String instrumentationName;
  private InstrumenterConstructor<REQUEST, RESPONSE> constructor =
      InstrumenterConstructor.internal();
  private SpanNameExtractor<? super REQUEST> spanNameExtractor;
  private SpanKind spanKind = SpanKind.INTERNAL;
  private StatusExtractor<? super REQUEST, ? super RESPONSE> statusExtractor =
      StatusExtractor.getDefault();
  private final List<AttributesExtractor<? super REQUEST, ? super RESPONSE>> attributesExtractors =
      new ArrayList<>();
  private ErrorCauseExtractor errorCauseExtractor = ErrorCauseExtractor.jdk();

  InstrumenterBuilder(OpenTelemetry openTelemetry, String instrumentationName) {
    this.openTelemetry = openTelemetry;
    this.instrumentationName = instrumentationName;

    // TODO: does this make any sense for a default? maybe it should be a mandatory param...
    spanNameExtractor = request -> instrumentationName;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> setSpanNameExtractor(
      SpanNameExtractor<? super REQUEST> spanNameExtractor) {
    this.spanNameExtractor = spanNameExtractor;
    return this;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> setClientSpanKind(TextMapSetter<REQUEST> setter) {
    this.spanKind = SpanKind.CLIENT;
    this.constructor =
        InstrumenterConstructor.propagatingToDownstream(openTelemetry.getPropagators(), setter);
    return this;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> setServerSpanKind(TextMapGetter<REQUEST> getter) {
    this.spanKind = SpanKind.SERVER;
    this.constructor =
        InstrumenterConstructor.propagatingFromUpstream(openTelemetry.getPropagators(), getter);
    return this;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> setSpanStatusExtractor(
      StatusExtractor<? super REQUEST, ? super RESPONSE> spanStatusExtractor) {
    this.statusExtractor = spanStatusExtractor;
    return this;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> addAttributesExtractor(
      AttributesExtractor<? super REQUEST, ? super RESPONSE> attributesExtractor) {
    this.attributesExtractors.add(attributesExtractor);
    return this;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> addAttributesExtractors(
      Collection<? extends AttributesExtractor<? super REQUEST, ? super RESPONSE>>
          attributesExtractors) {
    this.attributesExtractors.addAll(attributesExtractors);
    return this;
  }

  public InstrumenterBuilder<REQUEST, RESPONSE> setErrorCauseExtractor(
      ErrorCauseExtractor errorCauseExtractor) {
    this.errorCauseExtractor = errorCauseExtractor;
    return this;
  }

  public Instrumenter<REQUEST, RESPONSE> build() {
    return constructor.create(
        openTelemetry.getTracer(instrumentationName, InstrumentationVersion.VERSION),
        spanNameExtractor,
        spanKind,
        statusExtractor,
        attributesExtractors,
        errorCauseExtractor);
  }

  private interface InstrumenterConstructor<RQ, RS> {
    Instrumenter<RQ, RS> create(
        Tracer tracer,
        SpanNameExtractor<? super RQ> spanNameExtractor,
        SpanKind spanKind,
        StatusExtractor<? super RQ, ? super RS> statusExtractor,
        Iterable<? extends AttributesExtractor<? super RQ, ? super RS>> extractors,
        ErrorCauseExtractor errorCauseExtractor);

    static <RQ, RS> InstrumenterConstructor<RQ, RS> internal() {
      return Instrumenter::new;
    }

    static <RQ, RS> InstrumenterConstructor<RQ, RS> propagatingToDownstream(
        ContextPropagators propagators, TextMapSetter<RQ> setter) {
      return (tracer, spanName, spanKind, spanStatus, attributes, errorCauseExtractor) ->
          new ClientInstrumenter<>(
              tracer,
              spanName,
              spanKind,
              spanStatus,
              attributes,
              errorCauseExtractor,
              propagators,
              setter);
    }

    static <RQ, RS> InstrumenterConstructor<RQ, RS> propagatingFromUpstream(
        ContextPropagators propagators, TextMapGetter<RQ> getter) {
      return (tracer, spanName, spanKind, spanStatus, attributes, errorCauseExtractor) ->
          new ServerInstrumenter<>(
              tracer,
              spanName,
              spanKind,
              spanStatus,
              attributes,
              errorCauseExtractor,
              propagators,
              getter);
    }
  }
}
