package com.alongwang.brave.instrumentation.rocketmq;

import brave.Span;
import brave.internal.Nullable;
import brave.propagation.CurrentTraceContext;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;

final class TracingCallback {
    static SendCallback create(@Nullable SendCallback delegate, Span span, CurrentTraceContext current) {
        if (delegate == null) return new FinishSpan(span);
        return new DelegateAndFinishSpan(delegate, span, current);
    }

    static class FinishSpan implements SendCallback {
        final Span span;

        FinishSpan(Span span) {
            this.span = span;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            span.finish();
        }

        @Override
        public void onException(Throwable e) {
            span.error(e);
            span.finish();
        }
    }

    static final class DelegateAndFinishSpan extends FinishSpan {
        final SendCallback delegate;
        final CurrentTraceContext current;

        DelegateAndFinishSpan(SendCallback delegate, Span span, CurrentTraceContext current) {
            super(span);
            this.delegate = delegate;
            this.current = current;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            try (CurrentTraceContext.Scope ws = current.maybeScope(span.context())) {
                delegate.onSuccess(sendResult);
            } finally {
                super.onSuccess(sendResult);
            }
        }

        @Override
        public void onException(Throwable e) {
            try (CurrentTraceContext.Scope ws = current.maybeScope(span.context())) {
                delegate.onException(e);
            } finally {
                super.onException(e);
            }
        }
    }
}
