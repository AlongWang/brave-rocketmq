package rocketmq;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.messaging.MessagingRequest;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TracingMessageListenerConcurrently implements MessageListenerConcurrently {
    final MessageListenerConcurrently delegate;
    final RocketMQTracing rocketMQTracing;
    final Tracing tracing;
    final Tracer tracer;
    final TraceContext.Extractor<RocketMQConsumerRequest> extractor;
    final SamplerFunction<MessagingRequest> sampler;
    final TraceContext.Injector<RocketMQConsumerRequest> injector;
    final String remoteServiceName;
    final TraceContextOrSamplingFlags emptyExtraction;

    public static TracingMessageListenerConcurrently create(MessageListenerConcurrently listener, RocketMQTracing mqTracing) {
        return new TracingMessageListenerConcurrently(listener, mqTracing);
    }

    TracingMessageListenerConcurrently(MessageListenerConcurrently consumer, RocketMQTracing mqTracing) {
        this.delegate = consumer;
        this.rocketMQTracing = mqTracing;
        this.tracing = mqTracing.messagingTracing.tracing();
        this.extractor = mqTracing.consumerExtractor;
        this.sampler = mqTracing.consumerSampler;
        this.injector = mqTracing.consumerInjector;
        this.remoteServiceName = mqTracing.remoteServiceName;
        this.emptyExtraction = mqTracing.emptyExtraction;
        this.tracer = mqTracing.tracer;
    }

    void setConsumerSpan(String topic, Span span) {
        span.name("poll").kind(Span.Kind.CONSUMER).tag(RocketMQTags.ROCKETMQ_TOPIC_TAG, topic);
        if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);
    }


    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        Map<String, Span> consumerSpansForTopic = new LinkedHashMap<>();
        long timestamp = 0L;
        if (!tracing.isNoop()) {
            for (MessageExt msg : msgs) {
                RocketMQConsumerRequest request = new RocketMQConsumerRequest(msg);
                TraceContextOrSamplingFlags extracted = rocketMQTracing.extractAndClearTraceIdProperties(extractor, request, msg.getProperties());

                Span span = consumerSpansForTopic.get(msg.getTopic());
                if (span == null) {
                    span = rocketMQTracing.nextMessagingSpan(sampler, request, extracted);
                    if (!span.isNoop()) {
                        setConsumerSpan(msg.getTopic(), span);
                        if (timestamp == 0L) {
                            timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
                        }
                        span.start(timestamp);
                    }
                    consumerSpansForTopic.put(msg.getTopic(), span);
                }

                injector.inject(span.context(), request);
            }
        }

        Map<String, Tracer.SpanInScope> consumerScopeForTopic = new LinkedHashMap<>();
        for (String topic : consumerSpansForTopic.keySet()) {
            Tracer.SpanInScope ws = tracer.withSpanInScope(consumerSpansForTopic.get(topic));
            consumerScopeForTopic.put(topic, ws);
        }

        try {
            return this.delegate.consumeMessage(msgs, context);
        } finally {
            for (Tracer.SpanInScope ws : consumerScopeForTopic.values()) {
                ws.close();
            }
            for (Span span : consumerSpansForTopic.values()) {
                span.finish(timestamp);
            }
        }
    }
}
