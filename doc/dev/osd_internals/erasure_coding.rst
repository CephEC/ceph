==============================
Erasure Coded Placement Groups
==============================

Glossary
--------

*chunk* 
   when the encoding function is called, it returns chunks of the same
   size. Data chunks which can be concatenated to reconstruct the original
   object and coding chunks which can be used to rebuild a lost chunk.

*chunk rank*
   the index of a chunk when returned by the encoding function. The
   rank of the first chunk is 0, the rank of the second chunk is 1
   etc.

*stripe* 
   when an object is too large to be encoded with a single call,
   each set of chunks created by a call to the encoding function is
   called a stripe.

*shard|strip*
   an ordered sequence of chunks of the same rank from the same
   object.  For a given placement group, each OSD contains shards of
   the same rank. When dealing with objects that are encoded with a
   single operation, *chunk* is sometime used instead of *shard*
   because the shard is made of a single chunk. The *chunks* in a
   *shard* are ordered according to the rank of the stripe they belong
   to.

*K*
   the number of data *chunks*, i.e. the number of *chunks* in which the
   original object is divided. For instance if *K* = 2 a 10KB object
   will be divided into *K* objects of 5KB each.

*M* 
   the number of coding *chunks*, i.e. the number of additional *chunks*
   computed by the encoding functions. If there are 2 coding *chunks*, 
   it means 2 OSDs can be out without losing data.

*N*
   the number of data *chunks* plus the number of coding *chunks*, 
   i.e. *K+M*.

*rate*
   the proportion of the *chunks* that contains useful information, i.e. *K/N*.
   For instance, for *K* = 9 and *M* = 3 (i.e. *K+M* = *N* = 12) the rate is 
   *K* = 9 / *N* = 12 = 0.75, i.e. 75% of the chunks contain useful information.

The definitions are illustrated as follows (PG stands for placement group):
::
 
                 OSD 40                       OSD 33
       +-------------------------+ +-------------------------+
       |      shard 0 - PG 10    | |      shard 1 - PG 10    |
       |+------ object O -------+| |+------ object O -------+|
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   0   |||    stripe 0         ||| |||    stripe 0         ||| 
       ||+---------------------+|| ||+---------------------+||
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   1   |||    stripe 1         ||| |||    stripe 1         |||
       ||+---------------------+|| ||+---------------------+||
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   2   |||    stripe 2         ||| |||    stripe 2         |||
       ||+---------------------+|| ||+---------------------+||
       |+-----------------------+| |+-----------------------+|
       |         ...             | |         ...             |
       +-------------------------+ +-------------------------+

Weave: background EC object packing (experimental)
--------------------------------------------------

Weave interleaves stable objects into EC stripes.
Foreground writes use the ordinary EC transaction path and acknowledge its
normal durable completion. They do not wait for other objects or change the
pool's stripe unit. Background conversion
groups stable head objects from the same PG into an internal EC Volume.

For a pool with ``k`` data shards and fixed stripe unit ``U``, a newly built group
contains exactly ``k`` members; deletion may later leave fewer active members.
Its member slot size is
``L = round_up(max(member_sizes), U)``. Successive ``U``-byte units of one member
occupy successive EC stripes on the same data shard. The padded logical Volume
size is ``k * L``; the normal EC encoder supplies its parity shards.

Candidates are indexed by committed size and considered in descending-size
windows. A window is accepted only when its members have been quiet long enough,
its padded size fits the Volume limit, and
``100 * k * L <= (100 + padding_percent) * sum(member_sizes)``.
Large outliers are skipped rather than forcing an inefficient partial group.
Active watchers and existing snapshot clones prevent selection.

Code boundaries
~~~~~~~~~~~~~~~

Feature-specific code lives in ``src/osd/weave`` in the ``ceph::weave``
namespace. Types use the ``Weave`` prefix. Configuration uses ``osd_weave_*``
and the admin command is ``weave cleanup``. Physical Volumes use the
``.ceph-internal-weave`` namespace and member xattrs use the ``weave.`` prefix.
The former aggregate implementation, option names, command and disk identifiers
are not supported; there are no aliases, fallback readers or migration paths.

Native integration has three entry points:

* ``WeaveService`` is owned by ``OSDService``. It owns the shared scheduler,
  daily reclaim timer and reclaim-pass lifetime. OSD supplies a snapshot-based
  dispatcher which acquires each PG lock outside the OSD lock.
* ``WeavePGController`` is a PG-serialized facade with a private implementation.
  It owns the candidate index, catalog, reservations and deferred requests.
  Request hooks return an explicit native/translated/deferred/rejected
  disposition. Native transaction hooks remain at their original commit points.
* ``WeaveECAdapter`` handles member read coordinates, reconstruction and data
  classes. Native EC code does not encode Weave's private extent flags.

``WeavePGInterface`` is the capability interface implemented by a native
``PrimaryLogPG::WeavePGAdapter``. It returns object-state values and provides
versioned I/O, deferred callbacks, scheduling and request requeueing. Core Weave
code has no PG/ObjectContext pointer and no native friendship. Adapter I/O
callbacks must not run inline: their returned transaction ID authenticates
internal I/O before it enters the PG. Completions are dispatched under the PG
lock, with PG references retaining their owner across asynchronous work.

Implementation classes live in ``src/osd/weave/detail``:

* ``WeaveCandidateIndex`` and ``WeaveLayout`` own selection and layout arithmetic.
* ``WeaveCatalog`` owns authoritative member mappings and immutable metadata
  views. Readers retain their published view when a newer one replaces it.
* ``WeaveMemberTranslator`` borrows a const catalog. It owns request translation,
  logical STAT/xattr/version handling, delete preparation and reply restoration;
  it cannot publish or remove mappings. ``WeaveRequestContext`` retains the
  logical request and its pinned metadata for retry/reply handling.
* ``WeaveConversionJob`` owns the packing/unpacking state machine, payloads and
  one conversion lease. An immutable ``Mode::kPack`` or ``Mode::kUnpack`` selects
  the conversion direction; ``Stage`` tracks execution progress. Both directions
  carry members. An empty unpack volume skips restoration and goes to deletion.
  Completion and cancellation share a single terminal transition. PG generation
  and per-I/O sequence checks reject stale or duplicate completions. Controller
  reservations are tagged with job identities, so an old task cannot release a
  newer task's reservation. Publication revalidates sources under the PG lock.
* ``WeaveWorker`` runs one shared OSD worker. ``WeaveReclaimTimer`` computes
  daily UTC deadlines without owning a thread. Both are hidden by the service.

``WeaveTransaction`` borrows projected-attribute access and mutation functions
from the native transaction. The facade owns the metadata key and encoding;
``on_commit`` applies the individual member deletion to the catalog. This keeps
concurrent deletes from overwriting one another with older full mappings.

The ``weave_boundaries`` test checks public-header dependencies and native
includes. ``OpRequest.cc`` is the sole native include exception for allocating
and destroying the private request context. Deterministic ``FakeWeavePG`` tests cover
conversion failure, cancellation, stale completions, publication ordering and
projected member deletion without creating a live PG or Objecter.

Configured data classes (``osd_weave_data_classes``) consume object bytes
with ``ClsParmContext`` rather than the native PG context used by
``cls_cxx_*``. The gathered and shard-local paths share one data-class executor.
Unsupported native operations on an aggregated member first return the group
to ordinary storage.

Conversion and mutation
~~~~~~~~~~~~~~~~~~~~~~~

The forward flow is: reserve members, read version-checked data and xattrs,
build the interleaved payload on the shared worker, durably write the Volume,
revalidate and publish its mapping, then retire the original objects. Reads
continue using ordinary objects until publication.

A supported member DELETE updates only ``_volume_meta`` through the ordinary EC
transaction, deduplication and durable completion path. Its metadata read/modify/
write uses an exclusive object lock, including for a pure DELETE. The payload
and parity remain unchanged until cleanup. A final DELETE may follow supported
reads or assertions; unsupported compounds use the native conversion fallback.

Other mutations take the reverse flow: read the Volume, durably materialize
only its active members as ordinary EC objects, detach the mapping, retire the
Volume, then replay queued requests. This retains whole-Volume read amplification;
single-member write COW is not implemented. Logical reads use the mapping until
ordinary copies are ready. Requests queued before conversion restore their
logical identity and original inputs before checking reservations and mappings.
Native shadows from canceled transitions are drained before logical deletion.

The defaults are a 1 MiB minimum object size, 30 seconds of quiet time,
5 seconds between scans, a 64 MiB padded Volume limit, one concurrent conversion
per OSD and at most 10 percent padding. These are controlled respectively by
``osd_weave_min_object_size``, ``osd_weave_quiet_period``,
``osd_weave_scan_interval``, ``osd_weave_max_volume_size``,
``osd_weave_max_concurrent`` and ``osd_weave_max_padding_percent``.
``osd_weave_enabled`` enables this experimental path;
``osd_weave_background_enabled`` enables candidate scans. Setting the
concurrency limit to zero also blocks materialization admission, so disable
candidate scans instead when only background aggregation should stop.

Sparse cleanup
~~~~~~~~~~~~~~

``osd_weave_cleanup_time`` accepts strict ``HH:MM`` in UTC. Its default is
empty, disabling automatic cleanup. Enabling or changing the time schedules
the next future occurrence, without startup catch-up. Clock jumps do not queue
one pass for each missed day. The manual command works even with an empty time::

  ceph config set osd osd_weave_cleanup_time 02:00
  ceph config set osd osd_weave_cleanup_live_percent 50
  ceph tell osd.0 weave cleanup
  ceph tell 'osd.*' weave cleanup

Commands return ``{"status": "accepted"}`` or ``{"status": "already_running"}``,
not physical completion. Manual and scheduled requests share one pass per OSD,
the pass's initial threshold, and ``osd_weave_max_concurrent``. They operate
on primary, active, clean EC PGs. No additional worker or external cron is used.

Each pass enumerates Volume metadata, prioritizes empty Volumes, and selects
nonempty Volumes when ``100 * live_members <= k * live_percent``. The percentage
is occupied original data slots, not bytes. ``osd_weave_cleanup_live_percent``
defaults to 50 and accepts 0 through 100 inclusively: zero reclaims only empty
Volumes, while 100 also dismantles full Volumes.

Empty Volumes are deleted without payload reads. A selected sparse Volume is
read in full, but only surviving members are materialized; then the old Volume
is removed. Unselected payloads are not read. Restored objects retain their
contents, size, mtime, xattrs and logical user versions, and reenter ordinary
candidate selection with a new quiet period. There is no direct Volume merger.

Cleanup remains available with ordinary candidate scanning disabled. Re-enabling
``osd_weave_background_enabled`` wakes existing candidates without requiring
another foreground commit. Nonempty cleanup requires headroom for native copies
before the old Volume can be deleted; it is not in-place full-device compaction.

The metadata writer and reader use only Weave version 1, supporting sparse/empty
Volumes and persisting each member's user version and original snapshot sequence.
Other structure versions are rejected, even when they claim compatibility.
Data written by the former aggregate implementation is not a supported input.
``MOSDOp`` and ``MOSDOpReply`` negotiate ``WEAVE_READ_REDIRECT``. Capable peers
use a version-10 tail carrying routing information; other peers retain the
native version-8 encoding. The old experimental version-9 translated-operation
payload is not used.

Redirected member reads
~~~~~~~~~~~~~~~~~~~~~~~

With ``osd_weave_redirect_reads`` enabled (the default), a primary in an
active, clean PG can return a ``WeaveReadRoute`` identifying the Volume, target
OSD/shard, OSDMap epoch and physical object version. The target must advertise
the protocol feature. The client resends the original logical object and
operations, with that route, directly to the data OSD. Read data or data-class
results return directly from that OSD to the client.

``WeaveReadSession`` owns the client's one-detour state. It never replaces the
logical object, operation vector or class/xattr arguments. Map/placement
changes, connection reset, stale metadata and local I/O errors return the
request to the primary without another redirect. The primary's existing read
and reconstruction paths remain the fallback.

``WeaveReadRouter`` owns server routing policy behind ``WeavePGController``.
The receiving OSD checks permissions on the original logical request, validates
local placement, the map epoch, object version and replica-read stability,
then reloads that Volume's durable metadata. It verifies logical membership
and the EC plugin's member-to-shard mapping before using ``WeaveMemberTranslator``
to translate the operations. It neither trusts client-supplied member extents
nor needs a replica-wide copy of the primary's catalog.

Only supported read-only head-object operations are redirected, including
logical STAT/xattrs and configured data classes. Writes, ordered reads, native
CLS operations and snapshot operations keep the primary path. A nonprimary
ECBackend only selects its local shard; it never consults primary-only peer
missing tables or attempts distributed reconstruction.

With the OSD, monitor, manager, vstart helper tools, Python RADOS extension and
``cls_openssl_md5`` built, run the disposable three-OSD integration test from
the source root::

  bash src/test/weave/vstart_direct_read.sh build

The test creates a k=2/m=1 pool, waits for background packing, verifies local
replica reads and MD5 execution, rejects direct requests to exercise client
fallback, stops a data OSD to verify degraded reconstruction, and checks reads
after foreground materialization. It stops the cluster on exit and retains
``build/weave-vstart.*/weave-direct-report.json`` and daemon/client logs.
``CEPH_PORT`` can select a different base port.

Committed member deletions on published Volumes survive metadata reload and
restart. The broader experiment still lacks a persistent publication protocol
for interrupted layout transitions and PG split/resharding semantics; this is
not a general crash-recovery guarantee.

Table of content
----------------

.. toctree::
   :maxdepth: 1

   Developer notes <erasure_coding/developer_notes>
   Jerasure plugin <erasure_coding/jerasure>
   High level design document <erasure_coding/ecbackend>
