# Brave RocketMQ
为rocketMQ 客户端提供集成zipkin链路监控能力

TracingProducer 为每条消息生成span数据，并将span数据放在消息的属性（properties）当中
TracingMessageListenerConcurrently 负责将消息属性（properties）中的span数据提取出来，并继续链路跟踪

## 要求



## 如何使用
### 配置
引入依赖
```xml
<dependency>
    <groupId>com.alongwang</groupId>
    <artifactId>brave-instrumentation-rocketmq</artifactId>
    <version>5.14.1-SNAPSHOT</version>
</dependency>
```

初始化MessagingTracing
```
MessagingTracing messagingTracing = MessagingTracing.create(tracing);
```
初始化RocketMQTracing
```
RocketMQTracing rocketMQTracing = RocketMQTracing.create(messagingTracing);
```

### 消费者
使用rocketMQ消费消息的时候，原先是通过编写继承MessageListenerConcurrently接口的类，通过将这个类注册到consumer中实现消息的消费及逻辑的处理。

该组件通过提供TracingMessageListenerConcurrently类实现监控相关逻辑的代理，通过TracingMessageListenerConcurrently.create方法，将继承MessageListenerConcurrently接口的类作为参数生成TracingMessageListenerConcurrently类，然后注册到consumer当中。

```
consumer.registerMessageListener(TracingMessageListenerConcurrently.create(mqListener, rocketMQTracing));
//或者用lambda表达式
//consumer.registerMessageListener(TracingMessageListenerConcurrently.create((msgs, context) -> { //do sth }, rocketMQTracing));
```
### 生产者
通过TracingProducer.create方法，将DefaultMQProducer实例作为参数，实现生产者监控相关逻辑的代理
```
TracingProducer tracingProducer = TracingProducer.create(producer, rocketMQTracing);
tracingProducer.start();
```