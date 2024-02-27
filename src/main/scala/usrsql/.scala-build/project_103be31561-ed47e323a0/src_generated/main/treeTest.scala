



final class treeTest$_ {
def args = treeTest_sc.args$
def scriptPath = """treeTest.sc"""
/*<script>*/
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
/*</script>*/ /*<generated>*/
/*</generated>*/
}

object treeTest_sc {
  private var args$opt0 = Option.empty[Array[String]]
  def args$set(args: Array[String]): Unit = {
    args$opt0 = Some(args)
  }
  def args$opt: Option[Array[String]] = args$opt0
  def args$: Array[String] = args$opt.getOrElse {
    sys.error("No arguments passed to this script")
  }

  lazy val script = new treeTest$_

  def main(args: Array[String]): Unit = {
    args$set(args)
    script.hashCode() // hashCode to clear scalac warning about pure expression in statement position
  }
}

export treeTest_sc.script as treeTest

