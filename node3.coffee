# This implementation attempts to impose a global ordering on operations to
# improve performance.

type = require('ottypes')['text-tp2']
{randomInt} = require 'ottypes/randomizer'
bSearch = require 'binary-search'

#hat = require 'hat'

#letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
letters = 'abcdef0123456789'
hat = (n) -> (letters[randomInt letters.length] for [0...n/4]).join ''

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

xorHash = (a, b) ->
  assert a.length is b.length
  out = Buffer a.length
  out[i] = _a ^ b[i] for _a,i in a
  out

isEmpty = (obj) ->
  for k of obj
    return false
  return true

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

    if @ops.length > 0 and (last = @ops[@ops.length - 1]).site == data.site
      last.op = type.compose last.op, data.op
      stats.compose++
    else
      @ops.push {site:data.site, op:data.op}

    @size++


  transformBy: (otherOp, site) -> # Transform the bubble by op data
    stats.transform += @ops.length * 2
    #console.log 'transformBy'.grey, @ops.length
    for d, i in @ops
      assert d.site != site
      #console.log 'transformBy'.grey, otherOp, d.op
      if i isnt @ops.length - 1
        next = type.transform otherOp, d.op, if d.site > site then 'left' else 'right'
      d.op = type.transform d.op, otherOp, if d.site < site then 'left' else 'right'
      otherOp = next

    return

  transform: (otherOp, site) ->
    stats.transform += @ops.length
    #console.log 'transform'.grey, @ops.length
    for d in @ops
      assert d.site != site
      #console.log 'transform'.grey, otherOp, d.op
      otherOp = type.transform otherOp, d.op, if site < d.site then 'left' else 'right'
    otherOp

  prune: (otherOp, site) ->
    stats.transform += @ops.length
    #console.log 'prune'.grey, @ops.length
    for d in @ops by -1
      #console.log 'prune'.grey, d.site, site, otherOp, d.op
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

  maxParentPos: (op) ->
    max = -1
    for id in op.parents
      assert id of @hPositions
      pos = @hPositions[id]
      max = pos if pos > max
    max

  # Compare two operations. Returns negative if a<b, positive if a>b.
  #
  # The node must contain all parents of both compared operations.
  cmp: (a, b) ->
    assert a.id != b.id
    maxA = @maxParentPos a
    maxB = @maxParentPos b
    #console.log 'cmp'.red, a.id, a.parents, maxA, b.id, b.parents, maxB
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
      ts: @clockSkew #+ Date.now()
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

    cmp = (a, b) => @hPositions[a] - @hPositions[b]
    f.sort cmp

    while f.length
      id = f.pop()
      continue if visited[id]
      visited[id] = yes

      op = @history[@hPositions[id]]

      if visit id, op
        f.push i for i in op.parents
      f.sort cmp

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
      #newOp.op = clone newOp.op
      newOps.push newOp

      newIds[id] = true

      f.push p for p in newOp.parents

    # The new ops aren't necessarily in order now. We want to process them in a
    # partial order.
    cmp = (a, b) -> a.pos - b.pos
    newOps.sort cmp
    #console.log 'pulling'.yellow, (o.id for o in newOps)

    #console.log 'new ops'.grey, newOps.length if newOps.length

    # Ok, we have our list of ops to pull.

    # We only need to calculate this if we have ops earlier than the earliest
    # op we're pulling that are missing in the remote peer.
    isLocal = {}
    # Just for stats
    localOnlyUsed = no

    # Position of first local operation
    firstLocal = Infinity
    
    #console.log 'common frontier'.grey, commonFrontier

    knownCommon = {}
    knownCommon[id] = true for id in commonFrontier

    # Find all the local ops.
    do =>
      f = {}
      f[id] = true for id in @frontier

      iter = 0
      for op in @history by -1
        break if isEmpty f

        iter++

        id = op.id
        delete f[id]

        if id of knownCommon
          #console.log 'traverse'.red, id
          knownCommon[i] = true for i in op.parents
        else
          #console.log 'traverse'.green, id
          isLocal[id] = true
          f[_id] = true for _id in op.parents
          firstLocal = @hPositions[id]# if @hPositions[id] < firstLocal
      #console.log 'traverse'.grey, 'iter', iter, '/', @history.length
      return

    #console.log 'local ops'.green, Object.keys isLocal

    bubble = Bubble()
    bubble.pos = firstLocal

    bsCmp = (a, b) => @cmp a, b

    newOpChunk = Bubble()
    chunkCatchup = (n) =>
      #console.log 'chunkCatchup'.grey, newOpChunk.pos, n
      return unless newOpChunk.size > 0
      assert n >= newOpChunk.pos >= 0
      for data in @history[newOpChunk.pos...n]
        #console.log newOpChunk, data
        data.op = newOpChunk.xf data.op, data.site
        #@hPositions[data.id] += newOpChunk.size
        #data.pos += newOpChunk.size
      newOpChunk.pos = n

    for newOp in newOps
      #console.log 'pulling'.yellow, newOp.site, newOp.id.green, newOp.parents

      #assert _id of @hPositions for _id in newOp.parents

      # Find where it needs to go in our op list.
      destPos = bSearch @history, newOp, bsCmp
      assert destPos < 0
      destPos = ~destPos

      assert destPos <= @history.length

      srcPos = newOp.pos

      #console.log 'destPos'.grey, destPos, 'srcPos'.grey, srcPos

      assert destPos >= srcPos

      chunkCatchup destPos

      # First we need to transform out anything thats not in the op's history
      #if !bufferEq newOp.xfHash, @history[srcPos].xfHash
      if destPos > srcPos
        assert !isEmpty isLocal

        localOnlyUsed = yes

#        assert bubble.pos < srcPos

        while bubble.pos < destPos
          data = @history[bubble.pos++]
          if isLocal[data.id]
            # The op is local only. Add it to the bubble.
            #console.log 'bubble.add'.cyan, data.id
            bubble.add data
          else
            # The op is included in the the remote, and the op we've been given
            # has been transformed.

            #console.log 'bubble prune swap'.cyan, data.id
            op_ = bubble.prune data.op, data.site
            bubble.transformBy op_, data.site
        
      #console.log 'destPos', destPos, 'srcPos', srcPos
      #console.log 'bubble.size', bubble.size
      assert srcPos + bubble.size is destPos
      newOp.op = bubble.xf newOp.op, newOp.site
      bubble.pos++

      # Splice in newOp to history and update everything.
      #
      # I could do this in one pass instead of one pass per op by composing
      # together all the operations and doing this slide as we iterate, but
      # this should be ok for now.
      #applied = {site:newOp.site, op:newOp.op}
      for data, i in @history[destPos...@history.length]
        @hPositions[data.id]++
        data.pos++
      #  transformX applied, data

      newOp.pos = destPos
      @history.splice destPos, 0, newOp
      @hPositions[newOp.id] = destPos
      #assert.equal Object.keys(@hPositions).length, @history.length
      @frontier = (id for id in @frontier when id not in newOp.parents)
      @frontier.push newOp.id

      if newOpChunk.size is 0
        newOpChunk.pos = destPos
      newOpChunk.add newOp
      newOpChunk.pos++

    if newOpChunk.size
      chunkCatchup @history.length
      @doc = type.apply @doc, op.op for op in newOpChunk.ops

  check: ->
    assert.equal Object.keys(@hPositions).length, @history.length

    checkDoc = type.create 'hi there'
    #console.log @hPositions, @history
    for h, i in @history
      assert.equal @hPositions[h.id], i
      assert.equal i, h.pos
 
      if i >= 1
        assert @cmp(@history[i-1], @history[i]) < 0

      checkDoc = type.apply checkDoc, h.op

    assert.deepEqual @doc, checkDoc


  sync: (other) ->
    #console.log 'sync', other.siteId, '->', @siteId
    #console.log (op.id for op in other.history)
    #console.log (op.id for op in @history)

    ###
    console.log 'v-------------------------v'.blue
    console.log 'me   '.green, (o.id for o in @history), 'f', @frontier
    console.log "  #{o.site} #{o.id}: ".green, o.parents, o.op for o in @history
    console.log 'other'.red, (o.id for o in other.history), 'f', other.frontier
    console.log "  #{o.site} #{o.id}: ".red, o.parents, o.op for o in other.history
    ###
    @pull other
    #other.pull @
    ###
    console.log 'me   '.green, (o.id for o in @history), 'f', @frontier
    console.log 'other'.red, (o.id for o in other.history), 'f', other.frontier
    console.log '^-------------------------^'.blue
    ###

    #@check()
    #other.check()

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

aTest = ->
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


bTest = ->
  # This group generates a swap.
  a.genOp()
  c.genOp()
  b.sync a
  b.sync c
  a.genOp()

  console.log 'a contains bc', (o.id for o in a.history)
  console.log 'b contains xb', (o.id for o in b.history)
  b.sync a


randTest = do ->
  start = process.hrtime()

  for i in [1..500]
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

console.log stats

#a.sync b
#b.sync a

#console.log a, b

#assert.deepEqual a.doc.data, b.doc.data
#assert.equal a.history.length, b.history.length


