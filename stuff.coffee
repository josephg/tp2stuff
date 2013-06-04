
# Swap arr[idx] with arr[idx + 1]
swap = (arr, i) ->
  console.log 'swap', arr[i], arr[i+1]
  tmp = arr[i]
  arr[i] = arr[i+1]
  arr[i+1] = tmp

transform = (op, other) ->
  console.log 'transform', op, other
  op

h1 = ['a', 'b', 'c', 'd', 'e']
h2 = ['x', 'a', 'y', 'b']

injest = (myOps, otherOps) ->
  mySet = {}
  mySet[op] = true for op in myOps

  otherSet = {}

  for otherOp, i in otherOps
    if !mySet[otherOp]
      # At all times, all ops < base are in other.
      base = nextViable = 0

      base++ while otherSet[myOps[base]]
      nextViable = base + 1
      console.log base, nextViable

      while base < i
        # scan from nextViable to find first op that otherOps has
        nextViable++ while !otherSet[myOps[nextViable]]

        # swap back.
        for k in [nextViable - 1..base] by -1
          swap myOps, k
        console.log myOps
        base++
        nextViable++
       
      otherOp = transform otherOp, myOp for myOp in myOps[i...]
      myOps.push otherOp
      mySet[otherOp] = true

    otherSet[otherOp] = true


injest h1, h2

console.log h1
