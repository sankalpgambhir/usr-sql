sealed trait Projection

case class Base(name: String) extends Projection
case class Left(inner: Projection) extends Projection
case class Right(inner: Projection) extends Projection


@annotation.tailrec
def generateProjection_(base: Projection, index: Int, total: Int): Projection = 
  if total == 0 then
    // index == 0 too
    base
  else if index == total then
    generateProjection_(Right(base), index - 1, index - 1)
  else
    // index < total
    // index is the left sibling of a much larger subtree
    generateProjection_(Left(base), index, index)


println(generateProjection_(Base("t"), 0, 1))