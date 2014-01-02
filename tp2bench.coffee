type = require('ottypes')['text-tp2']
{randomInt} = require 'ottypes/randomizer'

time = (f) ->
  start = Date.now()
  f()
  end = Date.now()

  end - start

applyBench = (n = 1000000) ->
  doc = type.create()
  ops = for [0...n]
    [op, doc] = type.generateRandomOp doc
    op
   
  console.log "generated #{ops.length} ops"

  doc = type.create()

  total = ->
    doc = type.apply doc, op for op in ops

  console.log "#{total} ms"
  console.log "#{n / total * 1000} applys per second"


transformBench = (n = 1000000) ->
  doc = type.create 'hi there whats up its important this doc has some content in it'

  runners = (type.generateRandomOp(doc)[0] for [0...100])

  ops = for [0...(n/100)]
    [op, doc] = type.generateRandomOp doc
    op
   
  total = time ->
    for r in runners
      r = type.transform r, op, 'left' for op in ops

  console.log "#{total} ms for #{n} transforms"
  console.log "#{n / total * 1000} transforms per second"


#applyBench()
transformBench()



