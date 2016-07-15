# Pqbus
A simple event driven highly available message queue backed with PostgreSQL, focused on stability and performance.

[![Build Status](https://travis-ci.org/shanegibbs/pqbus.svg?branch=master)](https://travis-ci.org/shanegibbs/pqbus)
[![Dependency Status](https://dependencyci.com/github/shanegibbs/pqbus/badge)](https://dependencyci.com/github/shanegibbs/pqbus)

Push a message

```rust
let bus = pqbus::new("postgres://postgres@localhost/pqbus", "test").unwrap();
let queue = bus.queue("myqueue").unwrap();
queue.push("Hello World!");
```

Pop messages

```rust
let bus = pqbus::new("postgres://postgres@localhost/pqbus", "test").unwrap();
let queue = bus.queue("checker").unwrap();
queue.pop_callback(|body| {
    println!("Received: {}", body);
});
```

## Cli Interface

Push a message

```
echo "Hello World!" | pqbus push postgres://postgres@localhost/pqbus
```

Pop a message

```
pqbus pop postgres://postgres@localhost/pqbus
```

Watch and pop all messages from the queue

```
pqbus popall postgres://postgres@localhost/pqbus
```
