type = require('ottypes')['text-tp2']
{randomInt} = require 'ottypes/randomizer'
hat = require 'hat'
assert = require 'assert'

prune = (data, other) ->
  assert data.site != other.site
  #console.log 'prune', data.id, other.id
  data.op = type.prune data.op, other.op

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
  a.op = aop
  b.op = bop
  #console.log '        ->', a.op, b.op

s = 0

module.exports = node = (initial) ->

  mySet = {}

  frontier: []

  # Swap ops at idx and idx+1
  swap: (idx) ->
    s++
    #console.log 'swap', idx #, @history
    assert 0 <= idx < @history.length - 1

    a = @history[idx]
    b = @history[idx+1]

    assert a.id not in b.parents

    #console.log b, a
    prune b, a
    #console.log b, a
    transform a, b

    @history[idx] = b
    @history[idx+1] = a


  history: []
  doc: type.create initial

  siteId: hat(64)

  submit: (op) ->
    #console.log 'submit', @siteId, @doc.data, op
    @doc = type.apply @doc, op
    opData = {op, id:hat(32), site:@siteId, parents:@frontier}

    mySet[opData.id] = true
    @history.push opData
    @frontier = [opData.id]
    opData

  genOp: ->
    [op, doc] = type.generateRandomOp @doc
    #console.log 'genOp', op, @doc.data, '->', doc.data
    data = @submit op
    #console.log '  id:', data.id
    assert.deepEqual doc, @doc

  # Op must be at the latest local version.
  injestOp: (opData) ->
    #console.log 'apply to ', @siteId, @doc.data, opData.id, opData.op
    @doc = type.apply @doc, opData.op
    #console.log '   ->', @doc.data
    #assert.deepEqual @doc, opData.doc

    mySet[opData.id] = true
    #@history.push opData

    @frontier = (id for id in @frontier when id not in opData.parents)
    @frontier.push opData.id
    #@frontier = [opData.id]
    opData

  # Git style pull
  pull: (other) ->
    otherSet = {}

    # At all times, all ops < base are in other.
    base = nextViable = 0

    for otherOp, i in other.history
      #console.log 'a', i#, other.history
      if !mySet[otherOp.id]
        # we don't have an op from the remote.
        
        base++ while base < i and otherSet[@history[base].id]
        #console.log 'base = ', base, otherSet#, @history[base].id
        # base points to the first op that we have that remote is missing.
        # We need to swap that op out past position i.
        nextViable = base + 1
        #console.log base, nextViable

        while base < i
          #console.log base, i, myOps
          # scan from nextViable to find first op that otherOps has
          #console.log nextViable, @history
          while nextViable < @history.length and !otherSet[@history[nextViable].id]
            #console.log nextViable, @history
            nextViable++

          # swap back.
          for k in [nextViable - 1..base] by -1
            # swap @history[k] with @history[k+1]
            @swap k

          base++
          nextViable++
         
        #console.log myOps, otherOps
        # Strategy 1:

        #otherOp = transform otherOp, myOp for myOp in myOps[i...]
        #myOps.push otherOp

        # Strategy 2:
        otherOp = clone otherOp
        
        #console.log 'splicing', otherOp.id, 'at', i
        @history.splice i, 0, otherOp
        splicedOp = otherOp.op

        for k in [i+1...@history.length]
          #console.log 'other', otherOp
          transformX otherOp, @history[k]
          #console.log 'oth->', otherOp

        # end

        @injestOp otherOp
        otherOp.op = splicedOp


      otherSet[otherOp.id] = true



  sync: (other) ->
    console.log 'sync', other.siteId, '->', @siteId
    #console.log (op.id for op in other.history)
    #console.log (op.id for op in @history)

    @pull other

    if s
      console.log 'swaps:', s
      s = 0

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
###
a = node()
b = node()
c = node()
a.doc = b.doc = c.doc = type.create 'hi there' #type.generateRandomDoc()
###

###
a.genOp() # a
console.log 'a', (o.id for o in a.history)
b.genOp() # x
console.log 'x', (o.id for o in b.history)
b.sync a # xa
console.log 'xa', (o.id for o in b.history)
b.genOp() # xay
a.genOp() # ab
b.sync a # xayb
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

#a.sync b
#b.sync a

#console.log a, b

#assert.deepEqual a.doc.data, b.doc.data
#assert.equal a.history.length, b.history.length


