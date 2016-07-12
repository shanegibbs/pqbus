# Pqbus
A simple message queue backed with PostgreSQL.

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
