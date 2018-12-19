Actorkit
------------
[![Go Report Card](https://goreportcard.com/badge/github.com/gokit/actorkit)](https://goreportcard.com/report/github.com/gokit/actorkit)
[![Travis Build](https://travis-ci.org/gokit/actorkit.svg?branch=master)](https://travis-ci.org/gokit/actorkit#)

Actorkit is an experiement geared towards the fusion of actor based concurrency programming with different useful software architectural principles to development
a flexible, viable distributable, scalable foundation for building resilient services. 
It takes inspirations from projects like  [Akka](https://akka.io) and [Proto.Actor](http://proto.actor/) and others.

## Install

```bash
go get -u github.com/gokit/actorkit
```

## Architecture

![Actor System](./media/actors.png)

Actorkit is a fusion of CQRS, grains from Microsoft Orleans and actor-based message-passing principles within a
a single library geared towards the creation of scalable, distributed and resilient applications built on the concept of transparent,
addressable processing units or actors. It embraces the very nature of chaotic, failing system which is 
most transparent to the developer allowing the focus on creating systems able to resiliently function in such
environments. These details become part of the architecture and not just a after development process managemeent routine.
