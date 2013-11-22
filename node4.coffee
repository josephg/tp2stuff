# This script implements the op syncronization algorithm I figured out with
# Dominic Tarr.
#
# Imagine 2 peers each with operations that the other peer is missing.
# Operations are arranged in a DAG.
#
# Each peer has a *frontier*, which is the set of operations which have no
# dependants. This is the set of operations which any locally created operation
# will depend on.
#
# When a client connects to another client, it can perform one of 3 actions:
#
# - PULL
# - PUSH
# - SYNC
#
# PULL fetches all remote operations which are missing locally. PUSH sends all
# local operations which are missing remotely. It is equivalent to the remote
# peer doing a PULL. SYNC brings both peers in sync with one another. It is the
# equivalent of both peers doing a PULL.
#
# When doing a PULL:
#
# - For each operation in the source's frontier, if the operation is already
#   included in the destination model, discard it.
# - If the destination frontier is an ancestor of the source frontier, the
#   source should send all operations between fronteirs
# - If neither frontier contains the other frontier:
#   - The destination should send a bloom filter containing a selection of its
#     operations
#   - The source should use the bloom filter to find a common ancestor, then
#     send all operations since that ancestor
#
# If the destination has no operations, the source should simply send
# everything. This is a common case, and should be optimized for.
#
# PUSH/PULL/SYNC actions should scale with the number of sent ops, not the
# number of total ops. The number of round-trips should also scale well.

# So the protocol for PULL is:
#
# A: PULL. My frontier is [x,y,z]
#  
# (B) Find all ops that B has that A does not have. Strategies:
#   - If A's frontier is empty, send our entire database.
#   - If B's frontier is a subset of A's frontier, do nothing. A has all our ops.
#   - For each hash in A's frontier, if hash not in B's database, switch to bloom filter algorithm.
#   - Otherwise use scanning algorithm.
#
# Scanning algorithm ===
#   This requires that all operations in A's frontier are in B's database.
#
#   - Create two fronteirs (a and b). To start, a=A and b=B-A.
#   - While b not empty, scan through all operations in reverse order (requires
#     a chronilogical ordering which maintains partial order that if y is a child
#     of x, y is after x in sorted set).
#   - For each operation:
#     - If the operation is in a, remove it from b (if its there) and replace it in a with the operation's parents.
#     - Else if the operation is not in b, skip it
#     - Else add it to the list of ops to send to A and replace it in b with the operation's parents.
#
# B: The operations you need are [...]
#
#
# Bloom filter mode ===
#
# B: Get op bloom filter
# A: Here is a bloom filter marking which ops I have. The more accurate this bloom filter is, the fewer round-trips we'll need to do.
#
# (B)
#   - a=A's bloom filter. b=B's frontier. While b not empty, scan backwards through ops as in scanning algorithm above.
#   - For each operation:
#     - If operation not in b, skip it.
#     - Else if operation is in a (and operation's ancestors are in a with some confidence %), skip it.
#     - Else add it to the list of ops to send to A and replace it in b with the operation's parents.
#
# When B sends A the operations, its possible that A doesn't have some dependancies.
#
# A: OK <request complete>
#
# or
#
# A: Missing dependancies {...}
#
# (B) then runs the same bloom filter algorithm above, instead starting with b=A's missing dependancies.


# PUSH:
#
# B: PUSH. My frontier is [x,y,z]
# 
# (A) If A already has B's frontier in its database it replies with OK and the transaction is complete.
# Otherwise
# A: My frontier is [...]
#
# .. Then B continues with algorithm as per PULL.
#
#
# SYNC:
#
# A: SYNC. My frontier is [...]
# (B)
#   - If A's frontier == B's frontier, reply with OK and the transaction is done.
#   - If A's frontier is contained in B's database, send missing ops using scanning algorithm above.
#   - If A's frontier contains ops that B doesn't know about, continue:
# B: NEED_BLOOM_FILTER, my frontier is [...]
# (A)
#   - IF B's frontier is contained within A's database, send missing ops using scanning algorithm and say DONE. Do not send bloom filter.
#   - Else, .....




type = require('ottypes')['text-tp2']
{randomInt} = require 'ottypes/randomizer'
hat = require 'hat'
assert = require 'assert'
require 'colors'

clone = (obj) ->
  #console.log obj
  JSON.parse JSON.stringify obj

module.exports = node = (name) ->
  #doc: type.create initial

  siteId: name or hat(64)
  history: []
  frontier: []

  hPositions: {} # Map from id -> position in history of that op, if we have it.

  # Network messages. These methods should be exposed over an RPC stream.
  
  # Pull from this node. We need to send all operations that remote_frontier is missing to remote.
  n_pull: (remote, remote_frontier) ->
    if remote_fronteir.length is 0
      # Just reply with our entire history.
      remote.n_consume_ops @history
    else
      # If our frontier is a subset of remote's, we don't have to do anything. But 


  n_consume_ops: (remote, ops) ->
    # For now, I won't bother trying to actually do OT on the ops in the history list.
    for opData in ops when opData.id not in @hPositions
      console.log "Consuming #{opData.id}"
      for f in opData.frontier
        throw Error "Missing parent #{f} from op #{opData.id}" if f not of @hPositions

        # Remove this op's parents from our frontier.
        idx = @frontier.indexOf f
        if idx != -1
          @frontier.splice idx, 1

      @frontier.push opData.id

      @hPositions[opData.id] = @history.length
      @history.push opData

    return



  # Local submit.
  submit: (op) ->
    #console.log 'submit', @siteId, @doc.data, op
    #@doc = type.apply @doc, op

    # Position only relevant for originals.
    opData =
      id: hat(32)
      original: JSON.stringify op # Untransformed bytes for federation
      op: op # Transformed version of this op. Mutable.
      #opos: @history.length # Original position of this op when created.
      site: @siteId
      parents: @frontier.sort()

    @hPositions[opData.id] = @history.length
    @history.push opData

    @frontier = [opData.id]
    opData

  genOp: ->
    #[op, doc] = type.generateRandomOp @doc
    op = 'some op data'
    #console.log 'genOp', op, @doc.data, '->', doc.data
    data = @submit op
    #console.log '  id:', data.id
    #assert.deepEqual doc, @doc
    data

  check: ->
    assert.equal Object.keys(@hPositions).length, @history.length
    for h, i in @history
      assert.equal @hPositions[h.id], i


nodes = (node "node #{i}" for i in [1..5])
[a, b, c] = nodes
a.siteId = 'a'; b.siteId = 'b'; c.siteId = 'c'

###
a.genOp() # a
console.log 'a contains a', (o.id for o in a.history)
b.genOp() # x
console.log 'b contains x', (o.id for o in b.history)
b.sync a # ax
console.log 'b contains ax', (o.id for o in b.history)
b.genOp() # axy
console.log 'b contains axy', (o.id for o in b.history)
a.genOp() # ab
console.log 'a contains ab', (o.id for o in a.history)

b.sync a # abxy
console.log 'b contains abxy', (o.id for o in b.history)
a.genOp()
###

###
# This group generates a swap.
a.genOp()
c.genOp()
b.sync a
b.sync c
a.genOp()

console.log 'a contains bc', (o.id for o in a.history)
console.log 'b contains xb', (o.id for o in b.history)
b.sync a
###



###
start = process.hrtime()

console.log 'start'
for i in [1..100]
  console.log i
  for [1..2]
    n1 = nodes[randomInt nodes.length]
    n2 = nodes[randomInt nodes.length]
    n1.sync n2
  #sync (randomInt 3), (randomInt 3)
  nodes[randomInt nodes.length].genOp() for [1..10]
  #makeNode() for [1..3]

  if i % 30 is 0
    now = process.hrtime()
    diff = (now[0] - start[0]) + (now[1] - start[1]) / 1e9
    console.log diff
    start = now

b.sync a
c.sync b
a.sync c
b.sync a

assert.deepEqual a.doc.data, b.doc.data
assert.deepEqual b.doc.data, c.doc.data
console.log a.doc.data
###

#a.sync b
#b.sync a

#console.log a, b

#assert.deepEqual a.doc.data, b.doc.data
#assert.equal a.history.length, b.history.length


