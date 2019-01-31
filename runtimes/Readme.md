Runtimes
---------
So runtimes is a derivative of the initial idea of a flat queue-based event processing pipeline 
initially envisioned for actorkit due to the desire to allow actorkit sit ontop of any queue implementation
whether it supports [Consumers groups](https://blog.cloudera.com/blog/2018/05/scalability-of-kafka-messaging-using-consumer-groups/)
which is basically the concept know was [Competing Consumers](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html)
or not. 

Runtimes will be the entry point for all incoming events or messages into a actorkit system, they will provide
event locking and allocation where events which could be handled by multiple system is locked has taken by giving 
runtime instance deployed on a distributed setup where the desire is to have a guaranteed only processed once semantics
for a poll of runtimes with similar processsors for a giving event type plugged into a same queue or topic. This though 
is a configured behaviour and not the overall behaviour of the runtime, has the runtime could sit on top of kafka logs 
which provide partitions and consumer groups and resolutions to this type of needs and guarantees.


What we need then are the following properties for runtimes:

1. Addressable in a global space
2. Transparent as in location 
3. Generators and coordinates of created actors.
3. Managers of subscriptions on topics for events actors.
4. Managers and coordinators of actors lifecycle. 
6. Guarantors of message reception deliveries.