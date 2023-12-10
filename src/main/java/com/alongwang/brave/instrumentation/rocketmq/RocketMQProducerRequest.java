package com.alongwang.brave.instrumentation.rocketmq;

import brave.Span.Kind;
import brave.internal.Nullable;
import brave.messaging.ProducerRequest;
import brave.propagation.Propagation.RemoteGetter;
import brave.propagation.Propagation.RemoteSetter;
import org.apache.rocketmq.common.message.Message;

public class RocketMQProducerRequest extends ProducerRequest {

    static final RemoteGetter<RocketMQProducerRequest> GETTER =
            new RemoteGetter<RocketMQProducerRequest>() {
                public Kind spanKind() {
                    return Kind.PRODUCER;
                }

                public String get(RocketMQProducerRequest request, String fieldName) {
                    return request.delegate.getUserProperty(fieldName);
                }
            };

    static final RemoteSetter<RocketMQProducerRequest> SETTER =
            new RemoteSetter<RocketMQProducerRequest>() {
                public Kind spanKind() {
                    return Kind.PRODUCER;
                }

                public void put(RocketMQProducerRequest request, String fieldName, String value) {
                    request.delegate.putUserProperty(fieldName, value);
                }
            };


    final Message delegate;

    RocketMQProducerRequest(Message delegate) {
        if (delegate == null) throw new NullPointerException("delegate == null");
        this.delegate = delegate;
    }

    @Nullable
    @Override
    public String messageId() {
        return delegate.getKeys();
    }

    @Override
    public Kind spanKind() {
        return Kind.PRODUCER;
    }

    @Override
    public String operation() {
        return "send";
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
}
