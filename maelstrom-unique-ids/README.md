# Unique ID Generation

## Requirements

Run a 3-node cluster for 30 seconds and request new IDs at the rate of 1000 requests per second.
It checks for total availability and will induce network partitions during the test.
It will also verify that all IDs are unique.

## Solution

In a single node system generation of unique ids is achieved using a one way hashing function,
it is not necessary that this hashing function is cryptographically secure only that given an
input(such as a sequential number(system clock) or a pseudo-random bit/string) it produces a hash
uniformly distributed over the key space 2**(whatever bit) - 1 and the probability of a collision is extremely rare.

In the runtime of a distributed system where each node has its own view of the world there needs to be someway
of guaranteeing that input/seed of the hash function as the [system clock is unreliable](https://tigerbeetle.com/blog/three-clocks-are-better-than-one/).

The crux of the problem is having some way to co-ordinate between each server, let's say we have server A, B, C.

```
A -> generateUniqueID() 
B -> generateUniqueID()
C -> generateUniqueID()
```

If for whatever reason these services are called at the exact same time the likelihood of an ID generated on A
must not be generated on B or C at any point in time.

Alternatives:

1. Use a really large key space (2**128 - 1) - [a uuid.](https://en.wikipedia.org/wiki/Universally_unique_identifier)
2. Generate a [snowflake](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake) which combines
various properties for e.g
a timestamp + a logical_id (where it came from) + a sequence_id
3. Use a consistent hash ring

Luckily there are no requirements on space or ordering or searching or storage of the ids.
