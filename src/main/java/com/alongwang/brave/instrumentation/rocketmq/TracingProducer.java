package com.alongwang.brave.instrumentation.rocketmq;

import brave.Span;
import brave.Tracer;
import brave.internal.Nullable;
import brave.messaging.MessagingRequest;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class TracingProducer implements MQProducer {

    final MQProducer delegate;
    final RocketMQTracing mqTracing;
    final CurrentTraceContext currentTraceContext;
    final Tracer tracer;
    final Extractor<RocketMQProducerRequest> extractor;
    final SamplerFunction<MessagingRequest> sampler;
    final Injector<RocketMQProducerRequest> injector;
    @Nullable
    final String remoteServiceName;

    public static TracingProducer create(MQProducer producer, RocketMQTracing mqTracing) {
        return new TracingProducer(producer, mqTracing);
    }


    TracingProducer(MQProducer producer, RocketMQTracing mqTracing) {
        this.delegate = producer;
        this.mqTracing = mqTracing;
        this.currentTraceContext = mqTracing.messagingTracing.tracing().currentTraceContext();
        this.tracer = mqTracing.tracer;
        this.extractor = mqTracing.producerExtractor;
        this.sampler = mqTracing.producerSampler;
        this.injector = mqTracing.producerInjector;
        this.remoteServiceName = mqTracing.remoteServiceName;
    }

    @Override
    public void start() throws MQClientException {
        this.delegate.start();
    }

    @Override
    public void shutdown() {
        this.delegate.shutdown();
    }

    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.delegate.fetchPublishMessageQueues(topic);
    }

    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg);
    }

    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg, timeout);
    }

    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        sendInternal(msg, sendCallback);
    }

    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        sendInternal(msg, sendCallback, timeout);
    }

    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        sendOnewayInternal(msg);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg, mq);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg, mq, timeout);
    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        sendInternal(msg, mq, sendCallback);
    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        sendInternal(msg, mq, sendCallback, timeout);
    }

    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        sendOnewayInternal(msg, mq);
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg, selector, arg);
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg, selector, arg, timeout);
    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        sendInternal(msg, selector, arg, sendCallback);
    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        sendInternal(msg, selector, arg, sendCallback, timeout);
    }

    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, InterruptedException {
        sendOnewayInternal(msg, selector, arg);
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, Object arg) throws MQClientException {
        return this.delegate.sendMessageInTransaction(msg, tranExecuter, arg);
    }

    @Override
    public SendResult send(Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msgs);
    }

    @Override
    public SendResult send(Collection<Message> msgs, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msgs, timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msgs, mq);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msgs, mq, timeout);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        this.delegate.createTopic(key, newTopic, queueNum);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.delegate.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.delegate.searchOffset(mq, timestamp);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.delegate.maxOffset(mq);
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.delegate.minOffset(mq);
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.delegate.earliestMsgStoreTime(mq);
    }

    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.delegate.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
        return this.delegate.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.delegate.viewMessage(topic, msgId);
    }

    public SendResult sendInternal(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendInternal(msg, sendMsgTimeout);
    }

    public SendResult sendInternal(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Span span = doTracing(msg);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return this.delegate.send(msg, timeout);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public SendResult sendInternal(Message msg, MessageQueue mq) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        return sendInternal(msg, mq, sendMsgTimeout);
    }

    public SendResult sendInternal(Message msg, MessageQueue mq, long timeout) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Span span = doTracing(msg);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return this.delegate.send(msg, mq, timeout);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public SendResult sendInternal(Message msg, MessageQueueSelector selector, Object arg) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        return sendInternal(msg, selector, arg, sendMsgTimeout);
    }

    public SendResult sendInternal(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Span span = doTracing(msg);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return this.delegate.send(msg, selector, arg, timeout);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public void sendInternal(Message msg, SendCallback sendCallback) throws RemotingException, InterruptedException, MQClientException {
        sendInternal(msg, sendCallback, sendMsgTimeout);
    }

    public void sendInternal(Message msg, SendCallback sendCallback, long timeout) throws RemotingException, InterruptedException, MQClientException {
        Span span = doTracing(msg);
        Throwable error = null;
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            this.delegate.send(msg, TracingCallback.create(sendCallback, span, currentTraceContext), timeout);
        } catch (RuntimeException | Error e) {
            error = e;
            throw e;
        } finally {
            if (error != null) {
                span.error(error).finish();
            }
        }
    }

    public void sendInternal(Message msg, MessageQueue mq, SendCallback sendCallback) throws RemotingException, InterruptedException, MQClientException {
        sendInternal(msg, mq, sendCallback, sendMsgTimeout);
    }

    public void sendInternal(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws RemotingException, InterruptedException, MQClientException {
        Span span = doTracing(msg);
        Throwable error = null;
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            this.delegate.send(msg, mq, TracingCallback.create(sendCallback, span, currentTraceContext), timeout);
        } catch (RuntimeException | Error e) {
            error = e;
            throw e;
        } finally {
            if (error != null) {
                span.error(error).finish();
            }
        }
    }

    public void sendInternal(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws RemotingException, InterruptedException, MQClientException {
        sendInternal(msg, selector, arg, sendCallback, sendMsgTimeout);
    }

    public void sendInternal(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws RemotingException, InterruptedException, MQClientException {
        Span span = doTracing(msg);
        Throwable error = null;
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            this.delegate.send(msg, selector, arg, TracingCallback.create(sendCallback, span, currentTraceContext), timeout);
        } catch (RuntimeException | Error e) {
            error = e;
            throw e;
        } finally {
            if (error != null) {
                span.error(error).finish();
            }
        }
    }

    public SendResult sendInternal(Collection<Message> msgs) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        return sendInternal(msgs, sendMsgTimeout);
    }

    public SendResult sendInternal(Collection<Message> msgs, long timeout) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        List<Span> spanList = new ArrayList<>();
        List<Tracer.SpanInScope> wsList = new ArrayList<>();
        for (Message msg : msgs) {
            Span span = doTracing(msg);
            Tracer.SpanInScope ws = tracer.withSpanInScope(span);
            spanList.add(span);
            wsList.add(ws);
        }
        try {
            return this.delegate.send(msgs, timeout);
        } catch (RuntimeException | Error e) {
            spanList.forEach(span -> span.error(e));
            throw e;
        } finally {
            spanList.forEach(Span::finish);
            wsList.forEach(Tracer.SpanInScope::close);
        }
    }

    public SendResult sendInternal(Collection<Message> msgs, MessageQueue mq) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        return sendInternal(msgs, mq, sendMsgTimeout);
    }

    public SendResult sendInternal(Collection<Message> msgs, MessageQueue mq, long timeout) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        List<Span> spanList = new ArrayList<>();
        List<Tracer.SpanInScope> wsList = new ArrayList<>();
        for (Message msg : msgs) {
            Span span = doTracing(msg);
            Tracer.SpanInScope ws = tracer.withSpanInScope(span);
            spanList.add(span);
            wsList.add(ws);
        }
        try {
            return this.delegate.send(msgs, mq, timeout);
        } catch (RuntimeException | Error e) {
            spanList.forEach(span -> span.error(e));
            throw e;
        } finally {
            spanList.forEach(Span::finish);
            wsList.forEach(Tracer.SpanInScope::close);
        }
    }

    public void sendOnewayInternal(Message msg) throws RemotingException, InterruptedException, MQClientException {
        Span span = doTracing(msg);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            this.delegate.sendOneway(msg);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    void sendOnewayInternal(Message msg, MessageQueue mq) throws MQClientException,
            RemotingException, InterruptedException {
        Span span = doTracing(msg);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            this.delegate.sendOneway(msg, mq);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public void sendOnewayInternal(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        Span span = doTracing(msg);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            this.delegate.sendOneway(msg, selector, arg);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    private Span doTracing(Message msg) {
        RocketMQProducerRequest request = new RocketMQProducerRequest(msg);

        Span span;
        TraceContext maybeParent = currentTraceContext.get();
        if (maybeParent == null) {
            TraceContextOrSamplingFlags extracted =
                    mqTracing.extractAndClearTraceIdProperties(extractor, request, msg.getProperties());
            span = mqTracing.nextMessagingSpan(sampler, request, extracted);
        } else { // If we have a span in scope assume headers were cleared before
            span = tracer.newChild(maybeParent);
        }

        if (!span.isNoop()) {
            span.kind(Span.Kind.PRODUCER).name("send");
            if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);
            if (msg.getKeys() != null && !"".equals(msg.getKeys())) {
                span.tag(RocketMQTags.ROCKETMQ_KEY_TAG, msg.getKeys());
            }
            span.tag(RocketMQTags.ROCKETMQ_TOPIC_TAG, msg.getTopic());
            span.start();
        }

        injector.inject(span.context(), request);
        return span;
    }


    private int sendMsgTimeout = 3000;

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }
}
