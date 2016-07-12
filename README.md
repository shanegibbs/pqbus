# Pqbus
A simple event driven highly available message queue backed with PostgreSQL and focused on stability performance.

Initialize database

```
pqbus init postgres://postgres@localhost/pqbus
```

Run a publisher

```
pqbus publisher postgres://postgres@localhost/pqbus
```

Run a consumer

```
pqbus consumer postgres://postgres@localhost/pqbus
```
