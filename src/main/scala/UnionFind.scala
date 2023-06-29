package usrsql

class UnionFind[A](
    val parent: Map[A, A],
    val rank: Map[A, Int],
    val generator: A => Int
):
  def addElem(e: A): UnionFind[A] =
    if parent.contains(e) then this
    else
      UnionFind(parent.updated(e, e), rank.updated(e, generator(e)), generator)

  infix def +(e: A) = addElem(e)

  def find_(e: A): A =
    if parent(e) == e then e
    else find_(parent(e))

  def find(e: A): Option[A] =
    if parent.contains(e) then Some(find_(e))
    else None
  def apply(e: A) = find(e)

  def union(e1: A, e2: A): UnionFind[A] =
    val uf = (this + e1 + e2)
    if uf.rank(e1) >= uf.rank(e2) then
      UnionFind(uf.parent.updated(e2, e1), uf.rank, generator)
    else UnionFind(uf.parent.updated(e1, e2), uf.rank, generator)
