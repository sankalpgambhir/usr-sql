package usrsql.rewrite

import usrsql.language.USR.*
import usrsql.{MutableUnionFind as UF}
import collection.mutable.{HashSet as Set, HashMap as Map}

def lazyTranspose[A](seq: Seq[LazyList[A]]): LazyList[Seq[A]] =
  if (seq.forall(!_.isEmpty)) then
    seq.map(_.head) #:: lazyTranspose(seq.map(_.tail))
  else LazyList()

class ENode(val label: Label, val args: Seq[EClass])

class EClass(val elems: Set[ENode])

// invariants:
// - EClasses and ENodes form an acyclic DAG
// - the number of children of an ENode is fixed and corresponds to the arity of
//   the root label of term

class EGraph(rootTerm: Termable):
  type T = Termable

  val classes: Set[EClass] = Set.empty
  val nodes: Set[ENode] = Set.empty
  val emap: Map[ENode, EClass] = Map.empty
  val root: EClass = addTerm_(rootTerm)

  def addTerm(elem: T) =
    addTerm_(elem); ()

  def addTerm_(elem: T): EClass =
    val existing = find(elem)
    if existing.isDefined then
      existing.get
    else
      val newNode = ENode(elem.label, elem.args.map(addTerm_(_)))
      val newClass = EClass(Set(newNode))
      nodes += newNode
      classes += newClass
      emap += newNode -> newClass
      newClass
    
  def find(elem: T): Option[EClass] = 
    val classesToSearch = nodes.filter(_.label == elem.label).map(emap(_))
    classesToSearch.find(existsInClass(_, elem))

  def existsInClass(cl: EClass, elem: T): Boolean = 
    val nodes = cl.elems.filter(_.label == elem.label)
    nodes.exists(n => n.args.zip(elem.args).forall((c, t) => existsInClass(c, t)))

  def unify(l: T, r: T) =
    val lclass = addTerm_(l)
    val rclass = addTerm_(r)
    val union = EClass(lclass.elems ++ rclass.elems)
    classes -= lclass -= rclass += union
    // update nodes
    emap.transform((k, v) => if v == lclass || v == rclass then union else v)

  def generate: LazyList[T] =
    generateClass(root)

  private def generateClass(c: EClass): LazyList[T] =
    c.elems.map(generateNode(_)).reduce(_ ++ _)
  
  private def generateNode(n: ENode): LazyList[T] =
    n.label match
      case f: FunctionLabel => 
        val argLists = n.args.map(generateClass(_).asInstanceOf[LazyList[Term]])
        lazyTranspose(argLists).map(
          Function(f, _)
        )
      case a: AggregateLabel => 
        val argLists = n.args.map(generateClass(_).asInstanceOf[LazyList[Formula]])
        lazyTranspose(argLists).map(
          Aggregate(a, _)
        )
      case v: VariableTermLabel =>
        LazyList(TermVariable(v))
      case c: ConstantLabel =>
        LazyList(Constant(c.value))
      case p: PredicateLabel =>
        val argLists = n.args.map(generateClass(_).asInstanceOf[LazyList[Term]])
        lazyTranspose(argLists).map(
          Predicate(p, _)
        )
      case c: ConnectorLabel =>
        val argLists = n.args.map(generateClass(_).asInstanceOf[LazyList[Formula]])
        lazyTranspose(argLists).map(
          Connector(c, _)
        )
      case l: Lambda => 
        ???
      case v: VariableFormulaLabel =>
        LazyList(FormulaVariable(v))
    
end EGraph

class RewritingSystem(rootTerm: Termable) extends EGraph(rootTerm):
  type Rewrite = Tuple2[Termable, Termable]
  val rewrites: Set[Rewrite] = Set.empty

  def addRule(t: Rewrite) =
    rewrites += t

  def saturate: Unit =
    if rewrites.foldLeft(false)((b, r) => applyRule(r) || b) then
      saturate

  def applyRule(rw: Rewrite): Boolean =
    val matching = matchTermable(rw._1, root)
    if matching.isEmpty then
      false
    else
      val l = rw._1
      val r = rw._2
      val subst = matching.get
      val ls = l.substituted(subst)
      val rs = r.substituted(subst)
      if isSameTermable(ls, rs) then
        false
      else
        unify(ls, rs)
        true

  def matchTermable(r: T, c: EClass): Option[Substitution] = ???


end RewritingSystem

