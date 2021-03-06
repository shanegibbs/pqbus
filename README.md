# Pqbus
A simple event driven highly available message queue backed with PostgreSQL, focused on stability and performance.

[Documentation](https://shanegibbs.github.io/pqbus/pqbus/index.html)

[![Build Status](https://travis-ci.org/shanegibbs/pqbus.svg?branch=master)](https://travis-ci.org/shanegibbs/pqbus)
[![Dependency Status](https://dependencyci.com/github/shanegibbs/pqbus/badge)](https://dependencyci.com/github/shanegibbs/pqbus)
[![Coverage Status](https://coveralls.io/repos/github/shanegibbs/pqbus/badge.svg?branch=master)](https://coveralls.io/github/shanegibbs/pqbus?branch=master)

Push a message

```rust
let bus = pqbus::new("postgres://postgres@localhost/pqbus", "test").unwrap();
let queue = bus.queue("myqueue").unwrap();
queue.push("Hello World!");
```

Pop messages

```rust
let bus = pqbus::new("postgres://postgres@localhost/pqbus", "test").unwrap();
let mut queue = bus.queue("checker").unwrap();
for message in queue.messages() {
    println!("Received: {}", message);
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
