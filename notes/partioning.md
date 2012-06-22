Partitioning
==========

Yokozuna uses _doc-based_ partitioning, however a best effort will be
made to code partitioning in such a way that it might be easily
experimented with for future purposes.  The big reason to use
doc-based is because it makes entropy detection easiest.  Under this
scheme the document and it's entropy data can be atomically comitted.
This guarentees that the entropy data stored matches the document data
it represents.  This is discussed more in the _anti-entropy.md_
document.


