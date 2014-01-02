
# Swap arr[idx] with arr[idx + 1]
swap = (arr, i) ->
  console.log 'swap', arr[i], arr[i+1]
  tmp = arr[i]
  arr[i] = arr[i+1]
  arr[i+1] = tmp

transform = (op, other) ->
  console.log 'transform', op, other
  op

transformX = (op, other) ->
  [transform(op, other), transform(other, op)]

apply = (op) ->
  console.log 'apply', op

h1 = ['a', 'b', 'c', 'd', 'e']
h2 = ['x', 'a', 'y', 'b']

exports.injest = (myOps, otherOps) ->
  mySet = {}
  mySet[op] = true for op in myOps

  otherSet = {}

  for otherOp, i in otherOps
    #console.log myOps, otherOps
    if !mySet[otherOp]
      # At all times, all ops < base are in other.
      base = nextViable = 0 # Should this be i?

      base++ while otherSet[myOps[base]]
      nextViable = base + 1

      console.log base, nextViable

      while base < i
        #console.log base, i, myOps
        # scan from nextViable to find first op that otherOps has
        nextViable++ while !otherSet[myOps[nextViable]]

        # swap back.
        for k in [nextViable - 1..base] by -1
          swap myOps, k
        base++
        nextViable++
       
      #console.log myOps, otherOps
      # Strategy 1:

      #otherOp = transform otherOp, myOp for myOp in myOps[i...]
      #myOps.push otherOp

      # Strategy 2:
      
      myOps.splice i, 0, otherOp
      for k in [i+1...myOps.length]
        [otherOp, myOps[k]] = transformX otherOp, myOps[k]

      # end

      apply otherOp

      mySet[otherOp] = true


    otherSet[otherOp] = true


exports.injest h2, h1

#injest h2, h1

console.log h1
