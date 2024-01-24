# Unique ID Generation

## Requirements

Run a 3-node cluster for 30 seconds and request new IDs at the rate of 1000 requests per second.
It checks for total availability and will induce network partitions during the test.
It will also verify that all IDs are unique.

## Implicit Assumptions

- You shouldn't need persistence, writing to disk is relatively slow
- IDs should be unique across the cluster regardless of server restarts, garbage collection pauses, network partitions :)

## Solution

In a single node system generation of unique ids is typically achieved using either a growing
seq int, perhaps an `int64` or a one way hashing function(databases automagically do this),  it is often not necessary that this
hashing function is cryptographically secure(see [1]) only that given an input(such as a monotonic number(system clock),
sequential growing counter(for loop) or a pseudo-random bit/string) it produces a unique hash
uniformly distributed over the key space 2**(whatever bit) - 1 and the probability of a collision is extremely rare.

In the runtime of a distributed system where each node has its own view of the world there needs to be someway
of guaranteeing that input/seed of the hash function as the
[system clock is unreliable](https://tigerbeetle.com/blog/three-clocks-are-better-than-one/)

Alternatives:

1. Use a really large key space (2**128 - 1) - [a uuid.](https://en.wikipedia.org/wiki/Universally_unique_identifier)
2. Generate a [snowflake](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake) which combines
various properties for e.g a timestamp + a logical_id (where it came from) + a sequence_id,
Luckily there are no requirements on space or ordering or searching or storage of the ids.
3. Use a central server to generate ids.

## Relation to time and ordering

One of the seeds of a unique id generator are timestamps and hence the concept of time in a distributed system seems relevant but
these are distinct concepts. The use of a central authority such as an atomic clock or a "time server" or a lamport/logical clock is
about ordering of events and precision while uuids mostly care about uniqueness where time is a seed not necessarily monotonicity.

[1] <https://datatracker.ietf.org/doc/html/rfc4122#section-4.2.1>
