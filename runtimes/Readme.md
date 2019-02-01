# Runtimes

Runtimes is a derivative of the initial idea of a flat queue-based event processing pipeline.

In short, it allows actorkit to sit on top of any queue implementation, whilst still allowing a single
actor to be the only processor of any event even with multiple instances deployed within a distributed system. 


It replicates the competing consumers pattern when it's not natively supported by the queue system used.
If your curious here is more information on this approach:
[Consumers groups](https://blog.cloudera.com/blog/2018/05/scalability-of-kafka-messaging-using-consumer-groups/)
[Competing Consumers](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html)


## How it works

Runtimes is the entry point for all incoming events or messages into a actorkit system.

It provides:

- Event locking and allocation so that events can be handled by multiple systems.
  - **NOTE**: This is configurable. For example, if the runtime is sitting on top of Kafka we use its native partitions and consumer groups to achieve the same behaviour.

## Properties

The library provides the following properties for a developer. 

1. Addressable in a global space
2. Transparent as in location 
3. Generators and coordinates of created actors.
3. Managers of subscriptions on topics for events actors.
4. Managers and coordinators of actors lifecycle. 
6. Guarantors of message reception deliveries.
