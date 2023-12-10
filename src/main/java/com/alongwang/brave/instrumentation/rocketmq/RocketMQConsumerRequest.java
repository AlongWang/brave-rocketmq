package com.alongwang.brave.instrumentation.rocketmq;

import brave.Span;
import brave.internal.Nullable;
import brave.messaging.ConsumerRequest;
import brave.propagation.Propagation.RemoteGetter;
import brave.propagation.Propagation.RemoteSetter;
import org.apache.rocketmq.common.message.MessageExt;

public class RocketMQConsumerRequest extends ConsumerRequest {

    static final RemoteGetter<RocketMQConsumerRequest> GETTER =
            new RemoteGetter<RocketMQConsumerRequest>() {
                public Span.Kind spanKind() {
                    return Span.Kind.CONSUMER;
                }

                public String get(RocketMQConsumerRequest request, String fieldName) {
                    return request.delegate.getUserProperty(fieldName);
                }
            };

    static final RemoteSetter<RocketMQConsumerRequest> SETTER =
            new RemoteSetter<RocketMQConsumerRequest>() {
                public Span.Kind spanKind() {
                    return Span.Kind.CONSUMER;
                }

                public void put(RocketMQConsumerRequest request, String fieldName, String value) {
                    request.delegate.putUserProperty(fieldName, value);
                }
            };

    final MessageExt delegate;

    public RocketMQConsumerRequest(MessageExt delegate) {
        if (delegate == null) throw new NullPointerException("delegate == null");
        this.delegate = delegate;
    }

    @Override
    public String operation() {
        return "receive";
    }

    @Override
    public String channelKind() {
        return "topic";
    }

    @Override
    public String channelName() {
        return delegate.getTopic();
    }

    @Override
    public Object unwrap() {
        return delegate;
    }

    @Override
    public Span.Kind spanKind() {
        return Span.Kind.CONSUMER;
    }

    @Nullable
    @Override
    public String messageId() {
        return delegate.getKeys();
    }
}
