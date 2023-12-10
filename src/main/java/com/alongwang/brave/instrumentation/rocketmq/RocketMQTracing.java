package com.alongwang.brave.instrumentation.rocketmq;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.messaging.MessagingRequest;
import brave.messaging.MessagingTracing;
import brave.propagation.Propagation;
import brave.propagation.Propagation.Getter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class RocketMQTracing {

    static final Getter<Map<String, String>, String> GETTER = new Getter<Map<String, String>, String>() {
        public String get(Map<String, String> request, String key) {
            return request.get(key);
        }
    };

    public static RocketMQTracing create(Tracing tracing) {
        return newBuilder(tracing).build();
    }

    public static RocketMQTracing create(MessagingTracing tracing) {
        return newBuilder(tracing).build();
    }

    public static Builder newBuilder(Tracing tracing) {
        return newBuilder(MessagingTracing.create(tracing));
    }

    public static Builder newBuilder(MessagingTracing tracing) {
        return new Builder(tracing);
    }


    final MessagingTracing messagingTracing;
    final Tracer tracer;
    final String remoteServiceName;

    final TraceContext.Extractor<RocketMQProducerRequest> producerExtractor;
    final TraceContext.Extractor<RocketMQConsumerRequest> consumerExtractor;
    final TraceContext.Extractor<Map<String, String>> processorExtractor;
    final TraceContext.Injector<RocketMQProducerRequest> producerInjector;
    final TraceContext.Injector<RocketMQConsumerRequest> consumerInjector;
    final Set<String> traceIdHeaders;
    final TraceContextOrSamplingFlags emptyExtraction;
    final SamplerFunction<MessagingRequest> producerSampler, consumerSampler;

    RocketMQTracing(Builder builder) {
        this.messagingTracing = builder.messagingTracing;
        this.tracer = builder.messagingTracing.tracing().tracer();
        this.remoteServiceName = builder.remoteServiceName;
        Propagation<String> propagation = messagingTracing.propagation();
        this.producerExtractor = propagation.extractor(RocketMQProducerRequest.GETTER);
        this.consumerExtractor = propagation.extractor(RocketMQConsumerRequest.GETTER);
        this.processorExtractor = propagation.extractor(GETTER);
        this.producerInjector = propagation.injector(RocketMQProducerRequest.SETTER);
        this.consumerInjector = propagation.injector(RocketMQConsumerRequest.SETTER);
        this.producerSampler = messagingTracing.producerSampler();
        this.consumerSampler = messagingTracing.consumerSampler();

        this.traceIdHeaders = new LinkedHashSet<>(propagation.keys());

        this.emptyExtraction = propagation.extractor((c, k) -> null).extract(Boolean.TRUE);
    }

    public MessagingTracing messagingTracing() {
        return messagingTracing;
    }

    public <R> TraceContextOrSamplingFlags extractAndClearTraceIdProperties(TraceContext.Extractor<R> extractor, R request, Map<String, String> properties) {
        TraceContextOrSamplingFlags extracted = extractor.extract(request);
        // Clear any propagation keys present in the headers
        if (extracted.samplingFlags() == null) { // then trace IDs were extracted
            clearTraceIdHeaders(properties);
        }
        return extracted;
    }

    void clearTraceIdHeaders(Map<String, String> properties) {
        properties.keySet().removeIf(traceIdHeaders::contains);
    }

    public Span nextMessagingSpan(SamplerFunction<MessagingRequest> sampler, MessagingRequest request, TraceContextOrSamplingFlags extracted) {
        Boolean sampled = extracted.sampled();
        if (sampled == null && (sampled = sampler.trySample(request)) != null) {
            extracted = extracted.sampled(sampled.booleanValue());
        }
        return tracer.nextSpan(extracted);
    }

    public static final class Builder {
        final MessagingTracing messagingTracing;
        String remoteServiceName = "rocketMQ";

        Builder(MessagingTracing messagingTracing) {
            if (messagingTracing == null) throw new NullPointerException("messagingTracing == null");
            this.messagingTracing = messagingTracing;
        }

        Builder(RocketMQTracing rocketMQTracing) {
            this.messagingTracing = rocketMQTracing.messagingTracing;
            this.remoteServiceName = rocketMQTracing.remoteServiceName;
        }

        public Builder remoteServiceName(String remoteServiceName) {
            this.remoteServiceName = remoteServiceName;
            return this;
        }

        public RocketMQTracing build() {
            return new RocketMQTracing(this);
        }
    }
}
