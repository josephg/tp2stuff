type = require('ottypes')['text-tp2']
{randomInt} = require 'ottypes/randomizer'
hat = require 'hat'
assert = require 'assert'
require 'colors'

prune = (data, other) ->
  assert data.site != other.site
  #console.log 'prune', data.id, other.id
  data.op = type.prune data.op, other.op

# Swap ops at idx and idx+1
swap = (a, b) ->
  s++

  assert a.id not in b.parents

  #console.log b, a
  prune b, a
  #console.log b, a
  transform a, b


clone = (obj) ->
  #console.log obj
  JSON.parse JSON.stringify obj

transform = (data, other) ->
  assert data.site != other.site
  #console.log 'transform', data.id, other.id
  #console.log JSON.stringify data.op
  #console.log JSON.stringify other.op
  data.op = type.transform data.op, other.op, if data.site < other.site then 'left' else 'right'
  data

transformX = (a, b) ->
  console.log 'transformX'.grey, a.id.magenta, 'by', b.id.magenta
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

s = 0

module.exports = node = (initial) ->

  doc: type.create initial
  siteId: hat(64)
  history: []
  frontier: []

  hPositions: {} # Map from id -> position in history of that op, if we have it.

  swap: (idx) ->
    #console.log 'swap', idx #, @history
    assert 0 <= idx < @history.length - 1

    a = @history[idx]
    b = @history[idx+1]
    swap a, b
    @history[idx] = b
    @history[idx+1] = a

    @hPositions[b.id] = idx
    @hPositions[a.id] = idx + 1

  # Local submit.
  submit: (op) ->
    #console.log 'submit', @siteId, @doc.data, op
    @doc = type.apply @doc, op

    # Position only relevant for originals.
    opData =
      id: hat(32)
      original: JSON.stringify op # Untransformed bytes for federation
      op: op # Transformed version of this op. Mutable.
      opos: @history.length # Original position of this op when created.
      site: @siteId
      parents: @frontier.sort()

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
    data

  # Git style pull
  pull: (other) ->
    # Scan from the other node's frontier to find missing operations
    newIds = {}
    newOps = []

    f = other.frontier.slice()
    console.log 'f'.grey, f
    while f.length
      # Look at the first element in f. Throw it away if we've got it,
      # otherwise add it to newOps, update the frontier set and continue.
      id = f.pop()

      continue if id of @hPositions or id of newIds

      otherOp = other.history[other.hPositions[id]]
      newOp = {}
      newOp[k] = v for k, v of otherOp # Shallow clone
      newOp.op = JSON.parse newOp.original
      newOps.push newOp

      newIds[id] = true

      f.push p for p in newOp.parents

    # The new ops aren't necessarily in order now. We want to process them in a
    # partial order.
    console.log 'pulling'.yellow, (o.id for o in newOps)
    newOps.sort (a, b) -> other.hPositions[b.id] - other.hPositions[a.id]
    console.log 'sorted '.yellow, (o.id for o in newOps)


    console.log 'my originals', Object.keys @hPositions
    console.log 'newops', newOps
    console.log 'history'.cyan, (h.id for h in @history)
    # Ok, we have our list of ops to pull.
    for newOp in newOps by -1
      # Find the last parent.
      lastParentPos = -1
      for p in newOp.parents
        console.log 'parent'.red, p

        parentPos = @hPositions[p]
        assert parentPos?

        console.log 'parentPos'.red, parentPos

        lastParentPos = parentPos if parentPos > lastParentPos

      console.log 'lastParentPos'.green, lastParentPos

      assert lastParentPos >= newOp.opos - 1
      if lastParentPos > newOp.opos - 1 # -> while
        console.log 'swapsies!'.rainbow

      # Then transform out.
      pos = newOp.opos

      # The other way (easier way) would be to just transform the op out and
      # store it there.  If we're injesting a bunch of operations then that'll
      # be slow because we'll just end up swapping it back to its present
      # position again in the next loop iteration.
      @history.splice pos, 0, newOp
      @hPositions[newOp.id] = pos
      splicedOp = newOp.op

      for k in [pos+1...@history.length]
        #console.log 'other', otherOp
        transformX newOp, @history[k]
        @hPositions[@history[k].id]++
        #console.log 'oth->', otherOp

      @doc = type.apply @doc, newOp.op
      @frontier = (id for id in @frontier when id not in newOp.parents)
      @frontier.push newOp.id

      # Hax to avoid cloning the op again while we transform it out.
      newOp.op = splicedOp

      
    console.log '->story'.cyan, (h.id for h in @history)





  sync: (other) ->
    console.log 'sync', other.siteId, '->', @siteId
    #console.log (op.id for op in other.history)
    #console.log (op.id for op in @history)

    @pull other

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


nodes = (node 'hi there' for [1..10])
[a, b, c] = nodes
a.siteId = 'a'; b.siteId = 'b'; c.siteId = 'c'

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


#abc
#xayb


#console.log '-------'
#console.log a
#console.log b
#console.log '-------'



###
for [1..100]
  nodes[randomInt 10].sync nodes[randomInt 10]
  #sync (randomInt 3), (randomInt 3)
  nodes[randomInt 10].genOp() for [1..10]
  #makeNode() for [1..3]

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


