/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.instrumentation.api.tracer.AttributeSetter;

public abstract class AttributesExtractor<REQUEST, RESPONSE> {
  abstract void onStart(AttributeSetter setter, REQUEST request);

  abstract void onEnd(AttributeSetter setter, REQUEST request, RESPONSE response);

  protected static <T> void set(AttributeSetter setter, AttributeKey<T> key, T value) {
    if (value != null) {
      setter.setAttribute(key, value);
    }
  }
}
