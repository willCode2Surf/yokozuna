Anti-Entropy
==========

Each incoming KV object is converted into a Solr document with 3
fields.

1. `id` - The unique identifier for this document (since partitioning
          is per-index the key (from bucket-key) is unique.

2. `value` - The value of the `riak_object`.

3. `_entropy` - The corresponding entropy data in the form of a
                triplet `<iso8601> <partition> <id> <base64-obj-hash>`.

Using this data a Merkle Tree can be built representing representing
the data stored for a given partition before a given point in time.
This Merkle Tree can then be compared with a tree built from a KV
partition to detect entropy.  You could also build trees to compare
Solr partitions to each other to verify data is not divergent but not
for detecting missing data since partitions only partially overlap.
