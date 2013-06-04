hat = require 'hat'
random = require('srand').random
{injest} = require './stuff'

clients = for [1..3]
  frontier: ['x']
  history: []

nodes = {}

randomInt = (n) -> 0|(n * random())

makeNode = ->
  client = clients[randomInt 3]

  node =
    parents: client.frontier
    id: hat 30

  client.frontier = [node.id]
  client.history.push node.id

  nodes[node.id] = node

  node

sync = (aa, bb) ->
  return if aa == bb
  a = clients[aa]
  b = clients[bb]

  drawGraph aa
  drawGraph bb
  injest a.history, b.history
  injest b.history, a.history
  
  union = {}
  union[v] = true for v in a.frontier
  union[v] = true for v in b.frontier

  a.frontier = Object.keys union
  b.frontier = Object.keys union

  drawGraph aa
  drawGraph bb

gidx = 0
drawGraph = (c) ->
  g = require('graphviz').digraph 'ops'

  g.addNode 'x'
  for id, n of nodes
    g.addNode id
    for p in n.parents
      g.addEdge p, id

  client = clients[c]
  for n1, i in client.history[0...client.history.length - 1]
    n2 = client.history[i + 1]

    e = g.addEdge n1, n2
    e.set 'color', 'red'

  g.output 'png', "ops#{c} #{gidx++}.png"

for [1..10]
  sync (randomInt 3), (randomInt 3)
  makeNode() for [1..3]
