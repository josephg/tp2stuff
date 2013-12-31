# This implementation attempts to impose a global ordering on operations to
# improve performance.

type = require('ottypes')['text-tp2']
{randomInt} = require 'ottypes/randomizer'
bSearch = require 'binary-search'

hat = require 'hat'

#letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
#letters = 'abcdef0123456789'
#hat = (n) -> (letters[(Math.random() * letters.length) | 0] for [0...n]).join ''

assert = require 'assert'
#assert = (v) -> throw Error 'Assertion error' unless v
#assert.equal = (x, y) -> assert x == y
#assert.deepEqual = ->

require 'colors'
#require 'coffee-script'

stats =
  clone:0
  compose:0
  transform:0
  swap:0

xorHash = (a, b) ->
  assert a.length is b.length
  out = Buffer a.length
  out[i] = _a ^ b[i] for _a,i in a
  out

isEmpty = (obj) ->
  for k of obj
    return false
  return true

prune = (data, other) ->
  stats.transform++
  assert data.site != other.site
  #console.log 'prune', data.id, other.id
  data.op = type.prune data.op, other.op
  data.xfHash[i] ^= x for x, i of other.idBuf
  return

swap = (a, b) ->
  stats.swap++

  assert a.id not in b.parents

  #console.log b, a
  prune b, a
  #console.log b, a
  transform a, b

  a.pos++
  b.pos--

bufferEq = (a, b) -> a <= b && a >= b

clone = (obj) ->
  stats.clone++
  #console.log obj
  JSON.parse JSON.stringify obj

transform = (data, other) ->
  stats.transform++
  assert data.site != other.site
  #console.log 'transform', data.id, other.id
  #console.log JSON.stringify data.op
  #console.log JSON.stringify other.op
  data.op = type.transform data.op, other.op, if data.site < other.site then 'left' else 'right'
  data.xfHash[i] ^= x for x, i of other.idBuf
  data

transformX = (a, b) ->
  stats.transform+=2
  #console.log 'transformX'.grey, a.id.magenta, 'by', b.id.magenta
  assert a.site != b.site
  #console.log 'transformX', a.id, b.id
  #console.log 'transformX', a.op, b.op
  if a.site < b.site
    aop = type.transform(a.op, b.op, 'left')
    bop = type.transform(b.op, a.op, 'right')
  else
    aop = type.transform(a.op, b.op, 'right')
    bop = type.transform(b.op, a.op, 'left')
  #a = clone a
  #b = clone b
  a.op = aop; b.op = bop
  #console.log '        ->', a.op, b.op


# A bubble is a 'composed' set of operations which we push ('bubble') forward
# through history.
Bubble = ->
  pos: -1
  ops: [] # List of {site:_, op:_} pairs
  size: 0 # Number of ops in the bubble
  idBuf: null

  add: (data) -> # Add an operation to the bubble
    if @idBuf?
      @idBuf[i] ^= x for x, i of data.idBuf
    else
      @idBuf = Buffer data.idBuf

    if @ops.length > 0 and (last = @ops[ops.length - 1]).site == data.site
      last.op = type.compose last.op, data.op
      stats.compose++
    else
      @ops.push {site:data.site, op:data.op}

    @size++


  transformBy: (otherOp, site) -> # Transform the bubble by op data
    stats.transform += @ops.length
    for d in @ops
      assert d.site != site
      d.op = type.transform d.op, otherOp, if d.site < site then 'left' else 'right'

    return

  transform: (otherOp, site) ->
    stats.transform += @ops.length
    for d in @ops
      assert d.site != site
      otherOp = type.transform otherOp, d.op, if site < d.site then 'left' else 'right'
    otherOp

  prune: (otherOp, site) ->
    stats.prune += @ops.length
    for d in @ops
      assert d.site != site
      otherOp = type.prune otherOp, d.op
    otherOp

  xf: (otherOp, site) ->
    stats.transform += @ops.length * 2
    for d in @ops
      assert d.site != site

      if d.site < site
        next = type.transform otherOp, d.op, 'right'
        d.op = type.transform d.op, otherOp, 'left'
      else
        next = type.transform otherOp, d.op, 'left'
        d.op = type.transform d.op, otherOp, 'right'
      otherOp = next

    otherOp

  #pos: firstLocal # Position in the history that the bubble is at
  #op: data.op # I don't think I need to clone here because transform & compose do it
  #size: 1 # number of ops in the bubble
  #idBuf: Buffer(data.xfHash)



module.exports = node = (initial) ->

  doc: type.create initial
  siteId: hat(64)
  history: []
  frontier: [] # list of ids

  clockSkew: randomInt(30) * randomInt(30) # the real world is awful.

  hPositions: {} # Map from id -> position in history of that op, if we have it.

  # Swap ops at idx and idx+1
  swap: (idx) ->
    #console.log 'swap', idx #, @history
    assert 0 <= idx < @history.length - 1

    a = @history[idx]
    b = @history[idx+1]
    #console.log 'swap'.yellow, a, b
    swap a, b
    @history[idx] = b
    @history[idx+1] = a

    @hPositions[b.id] = idx
    @hPositions[a.id] = idx + 1

  composeRange: (start, end) ->
    assert start <= end

    if start is end
      return clone @history[start]
    else
      r = @history[start]
      # This could be made more efficient using the Alex Mah method from Wave.
      for i in [start+1..end]
        r = type.compose r, @history[i]
        stats.compose++
      r

  maxParentPos: (op) ->
    max = 0
    for id in op.parents
      assert id of @hPositions
      pos = @hPositions[id]
      max = pos if pos > max
    max

  # Compare two operations. Returns negative if a<b, positive if a>b. The node
  # must contain all parents of both compared operations.
  cmp: (a, b) ->
    assert a.id != b.id
    maxA = @maxParentPos a
    maxB = @maxParentPos b
    return maxA - maxB if maxA != maxB
    return a.ts - b.ts if a.ts != b.ts
    return (if a.site < b.site then -1 else 1)

  # Local submit.
  submit: (op) ->
    #console.log 'submit', @siteId, @doc.data, op
    @doc = type.apply @doc, op

    # Position only relevant for originals.
    opData =
      id: hat(32)
      op: op # Transformed version of this op. Mutable.
      pos: @history.length # position of this op in history
      site: @siteId
      ts: Date.now() + @clockSkew
      parents: @frontier.sort()
      _original: JSON.stringify op # Untransformed bytes for testing

    # The id as a buffer
    opData.idBuf = Buffer opData.id, 'hex'

    opData.xfHash = Buffer opData.idBuf

    @hPositions[opData.id] = @history.length
    @history.push opData

    @frontier = [opData.id]
    opData

  genOp: ->
    [op, doc] = type.generateRandomOp @doc
    #console.log 'genOp', op, @doc.data, '->', doc.data
    data = @submit op
    #console.log '  id:', data.id
    assert.deepEqual doc, @doc
    # New operation must be after the previous operation in the history according to cmp function.
    assert @cmp(@history[@history.length - 2], data) < 0 if @history.length >= 2
    data

  # Traverse operations from the specified frontier. visit function returns
  # truthy if we should expand the node
  traverse: (frontier, visit) ->
    f = frontier.slice()
    visited = {}

    while f.length
      id = f.pop()
      continue if visited[id]
      visited[id] = yes

      op = @history[@hPositions[id]]

      if visit id, op
        f.push i for i in op.parents

  # Git style pull
  pull: (other) ->
    # Scan from the other node's frontier to find missing operations
    newIds = {}
    newOps = []

    xf = 0

    f = other.frontier.slice()
    commonFrontier = [] # The frontier of operations both peers share
    
    #console.log 'f'.grey, f
    while f.length # can be rewritten to use traverse above
      # Look at the first element in f. Throw it away if we've got it,
      # otherwise add it to newOps, update the frontier set and continue.
      id = f.pop()
      continue if id of newIds

      if id of @hPositions
        commonFrontier.push id unless id in commonFrontier
        continue

      otherOp = other.history[other.hPositions[id]]
      newOp = {}
      newOp[k] = v for k, v of otherOp # Shallow clone
      newOp.op = clone newOp.op
      newOps.push newOp

      newIds[id] = true

      f.push p for p in newOp.parents

    # The new ops aren't necessarily in order now. We want to process them in a
    # partial order.
    console.log 'pulling'.yellow, (o.id for o in newOps)
    cmp = (a, b) => @cmp a, b
    newOps.sort cmp
    console.log 'sorted '.yellow, (o.id for o in newOps)

    console.log 'new ops'.grey, newOps.length if newOps.length

    #console.log 'my originals', Object.keys @hPositions
    #console.log 'newops', newOps
    #console.log 'history'.cyan, (h.id for h in @history)
    # Ok, we have our list of ops to pull.

    # We only need to calculate this if we have ops earlier than the earliest
    # op we're pulling that are missing in the remote peer.
    isLocal = {}
    # Just for stats
    localOnlyUsed = no

    # Position of first local operation
    firstLocal = Infinity
    
    @traverse @frontier, (id, op) =>
      if id not in commonFrontier
        isLocal[id] = true
        firstLocal = @hPositions[id] if @hPositions[id] < firstLocal
        yes
      else
        no

    bubble = Bubble()
    bubble.pos = firstLocal

    console.log 'Local ops'.green, isLocal

    for newOp in newOps
      # Find where it needs to go in our op list.
      destPos = bSearch @history, newOp, cmp
      assert destPos < 0
      destPos = ~destPos

      assert destPos <= @history.length

      srcPos = newOp.pos

      assert destPos >= srcPos

      console.log 'destPos', destPos, 'srcPos', srcPos

      # First we need to transform out anything thats not in the op's history
      #if !bufferEq newOp.xfHash, @history[srcPos].xfHash
      if destPos > srcPos
        assert !isEmpty isLocal

        localOnlyUsed = yes

#        assert bubble.pos < srcPos

        while bubble.pos < destPos
          bubble.pos++
          data = @history[bubble.pos]
          if isLocal[data.id]
            # The op is local only. Add it to the bubble.
            bubble.add data
          else
            # The op is included in the the remote, and the op we've been given
            # has been transformed.

            op_ = bubble.prune data.op, data.site
            bubble.transformBy op_, data.site
        
      assert srcPos + bubble.size is destPos
      newOp.op = bubble.xf newOp.op, newOp.site

      # Splice in newOp to history and update everything.
      #
      # I could do this in one pass instead of one pass per op by composing
      # together all the operations and doing this slide as we iterate, but
      # this should be ok for now.
      applied = {site:newOp.site, op:newOp.op}
      for data, i in @history[destPos...@history.length]
        @hPositions[data.id]++
        transformX applied, data

      @history.splice destPos, 0, newOp
      @hPositions[newOp.id] = destPos
      @doc = type.apply @doc, applied.op


  check: ->
    assert.equal Object.keys(@hPositions).length, @history.length
    for h, i in @history
      assert.equal @hPositions[h.id], i



  sync: (other) ->
    #console.log 'sync', other.siteId, '->', @siteId
    #console.log (op.id for op in other.history)
    #console.log (op.id for op in @history)

    @pull other
    #other.pull @
    @check()
    other.check()

    #if s
    #  console.log 'swaps:', s
    #  s = 0

    #for a in @history
    #  for b in other.history
    #    throw new Error 'same item' if a is b

    # ... and fix the frontier.
    ###
    f = {}
    f[id] = true for id in @frontier
    f[id] = true for id in other.frontier

    for data in @history
      delete f[id] for id in data.parents

    @frontier = Object.keys f
    ###


nodes = (node 'hi there' for [1..3])
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

start = process.hrtime()

for i in [1..1000]
  #console.log i
  for [1..2]
    n1 = nodes[randomInt nodes.length]
    n2 = nodes[randomInt nodes.length]
    n1.sync n2
  #sync (randomInt 3), (randomInt 3)
  nodes[randomInt nodes.length].genOp() for [1..10]
  #makeNode() for [1..3]

  if i % 100 is 0
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

console.log stats

#a.sync b
#b.sync a

#console.log a, b

#assert.deepEqual a.doc.data, b.doc.data
#assert.equal a.history.length, b.history.length


