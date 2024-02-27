package usrsql.language

import usrsql.Schema.DataType

object USR:
  sealed trait Label:
    val name: String

  sealed trait Termable:
    val label: Label
    val args: Seq[Termable]

  trait Term extends Termable
  case class Function(label: FunctionLabel, args: Seq[Term]) extends Term
  case class Aggregate(label: AggregateLabel, args: Seq[Formula]) extends Term
  case class TermVariable(label: VariableTermLabel) extends Term:
    val args: Seq[Term] = Seq.empty
    override def toString(): String = label.name
  case class Constant(data: DataType) extends Term:
    val label: Label = ConstantLabel(data)
    val args: Seq[Term] = Seq.empty

  sealed trait TermLabel extends Label
  case class FunctionLabel(val name: String, arity: Int) extends TermLabel:
    def apply(ts: Term*) =
      require(ts.length == arity)
      Function(this, ts)
  case class AggregateLabel(val name: String, arity: Int) extends TermLabel:
    def apply(fs: Formula*) =
      require(fs.length == arity)
      Aggregate(this, fs)
  case class VariableTermLabel(val name: String) extends TermLabel
  case class ConstantLabel(val value: DataType)
      extends TermLabel:
    val name: String = s"($value)"

  sealed trait Formula extends Termable
  case class Predicate(label: PredicateLabel, args: Seq[Term]) extends Formula
  case class Connector(label: ConnectorLabel, args: Seq[Formula]) extends Formula:
    override def toString(): String = 
      label match
        case Add => s"(${args.head} + ${args(1)})"
        case Mul => s"(${args.head} * ${args(1)})"
        case s: Sum => s"Σ ${s.variable}.(${args.head})"
        case OneLabel => "1"
        case ZeroLabel => "0"
        case _ => s"$label(${args.head})"
  case class FormulaVariable(label: VariableFormulaLabel) extends Formula:
    val args: Seq[Formula] = Seq.empty
  case class App(lambda: Lambda, to: TermVariable) extends Formula:
    val label: Label = lambda
    val args: Seq[Term] = Seq(to)
    override def toString(): String = s"$lambda ($to)"

  sealed trait FormulaLabel extends Label
  case class PredicateLabel(val name: String, val arity: Int)
      extends FormulaLabel:
    def apply(ts: Term*) =
      require(ts.length == arity)
      Predicate(this, ts)
    def unapply(c: Predicate): Option[Seq[Term]] =
      if c.label == this then
        Some(c.args)
      else 
        None
  sealed class ConnectorLabel(val name: String, val arity: Int)
      extends FormulaLabel:
    def apply(fs: Formula*) =
      require(fs.length == arity)
      Connector(this, fs)
    def unapply(c: Connector): Option[Seq[Formula]] =
      if c.label == this then
        Some(c.args)
      else 
        None
    override def toString(): String = name
  case class VariableFormulaLabel(val name: String) extends FormulaLabel
  case class Lambda(val variable: TermVariable, val body: Formula, val outputColumns: Seq[SQLIR.Value]) extends FormulaLabel:
    val name = s"λ $variable."
    override def toString(): String = s"$name $body"

  // predicates
  val Equality = PredicateLabel("Equality", 2)
  val Less = PredicateLabel("Less", 2)
  val LessOrEqual = PredicateLabel("LessOrEqual", 2)
  class Relation(name: String) extends PredicateLabel(name, 1)

  object Relation:
    def unapply(f: Formula): Option[(String, Term)] =
      f match
        case p: Predicate =>
          p.label match
            case r: Relation =>
              Some((r.name, p.args.head))
            case _ => None
        case _ => None

  // connectors
  val Add = ConnectorLabel("Add", 2)
  val Mul = ConnectorLabel("Mul", 2)
  val Not = ConnectorLabel("Not", 1)
  val Squash = ConnectorLabel("Squash", 1)
  class Sum(val variable: TermVariable)
      extends ConnectorLabel(s"Sum_$variable", 1)

  object Sum:
    def apply(variable: TermVariable, body: Formula): Formula = (new Sum(variable))(body)
    def unapply(f: Formula): Option[(TermVariable, Formula)] =
      f match
        case c: Connector =>
          c.label match
            case label: Sum => 
              Some((label.variable, c.args.head))
            case _ => None
        case _ => None
      
      

  private val ZeroLabel = ConnectorLabel("Zero", 0)
  private val OneLabel = ConnectorLabel("One", 0)
  val Zero = ZeroLabel()
  val One = OneLabel()

  extension (f1: Formula) {
    infix def +(f2: Formula) = Add(f1, f2)
    infix def *(f2: Formula) = Mul(f1, f2)
    def unary_! = Not(f1)
  }

  val Left = FunctionLabel("Left", 1)
  val Right = FunctionLabel("Right", 1)

  // substitutions :::
  type TermSubstitution = Map[VariableTermLabel, Term]
  type FormulaSubstitution = Map[VariableFormulaLabel, Formula]
  type Substitution = (TermSubstitution, FormulaSubstitution)

  extension (t: Termable)
    def substituted(subst: Substitution): Termable =
      t match
        case t: Term    => t.substituted(subst)
        case f: Formula => f.substituted(subst)

  extension (t: Term) def substituted(subst: Substitution): Term = ???
  extension (t: Formula) def substituted(subst: Substitution): Formula = ???

  def isSameTermable(t1: Termable, t2: Termable): Boolean = ???
end USR
