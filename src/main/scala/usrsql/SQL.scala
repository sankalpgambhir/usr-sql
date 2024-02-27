package usrsql
import Schema.DataType

/** Standard SQL Grammar
  */
object SQL {

  /** SQL Query, with a top-level select
    */
  sealed trait SelectStatement

  /** Internal SQL query grammar
    */
  sealed trait Query {
    override def toString(): String = prettyQuery(this)
  }

  case class SELECT(val selection: Projection, val from: FROM)
      extends Query
      with SelectStatement
  case class FROM(val clauses: Seq[(Query, QueryLabel)]) extends Query
  case class WHERE(val query: Query, val condition: Predicate) extends Query
  case class EXCEPT(val left: Query, val right: Query) extends Query
  case class UNIONALL(val left: Query, val right: Query) extends Query
  case class DISTINCT(val query: Query) extends Query

  /** An atomic query: a table reference
    */
  case class TableRef(val table: Table) extends Query

  extension (q: Query) {
    // convenient syntax for labelling
    infix def AS(name: String) = (q, QueryLabel(name))
    infix def AS(label: QueryLabel) = (q, label)
  }

  /** Boolean conditions and predicates
    */
  sealed trait Predicate

  case object TRUE extends Predicate
  case object FALSE extends Predicate

  sealed trait UnaryPredicate(val inner: Predicate) extends Predicate
  case class NOT(override val inner: Predicate) extends UnaryPredicate(inner)
  case class EXISTS(val inner: Query) extends Predicate

  sealed trait BinaryPredicate(val left: Predicate, val right: Predicate)
      extends Predicate
  case class AND(override val left: Predicate, override val right: Predicate)
      extends BinaryPredicate(left, right)
  case class OR(override val left: Predicate, override val right: Predicate)
      extends BinaryPredicate(left, right)

  /** Atomic formulas, in the form of a comparison (=, <, <=)
    *
    * @param left
    * @param right
    */
  sealed trait BinaryFormula(val left: Expression, val right: Expression)
      extends Predicate
  // =, <, <=
  case class EQ(override val left: Expression, override val right: Expression)
      extends BinaryFormula(left, right)
  case class LT(override val left: Expression, override val right: Expression)
      extends BinaryFormula(left, right)
  case class LE(override val left: Expression, override val right: Expression)
      extends BinaryFormula(left, right)

  /** valued expressions, i.e. "actual" typed values
    */
  sealed trait Expression

  case class ColumnRef(val t: Table, val c: Column) extends Expression
  case class Function(val label: FunctionLabel, val args: Seq[Expression])
      extends Expression
  case class Aggregate(val label: AggregateLabel, val arg: Query)
      extends Expression
  case class Constant[T <: DataType](val value: T) extends Expression

  /** Column references (used for selection)
    */
  sealed trait Projection

  /** SQL * operator
    */
  case object Star extends Projection

  /** A table projection, of the form t.c or t.*
    */
  case class ColumnProjection(val t: Table, val c: (Column | Star.type))
      extends Projection

  /** Labelled projection, of the form [expression] AS [label]
    *
    * @param exp
    * @param label
    */
  case class LabelledProjection(
      val expression: Expression,
      val label: ProjectionLabel
  ) extends Projection

  /** Compound projection, i.e., projecting multiple objects at once, of the
    * form [projection_1], [projection_2], ...
    *
    * @example
    *   "t1.c, t2.a, t3.*"
    * @param ps
    */
  case class CompoundProjection(val ps: Seq[Projection]) extends Projection

  // aliasing
  sealed trait Label(val name: String) {
    override def toString(): String = name
  }

  // query alias
  case class QueryLabel(override val name: String) extends Label(name)
  case class ProjectionLabel(override val name: String) extends Label(name)

  // table and column references
  case class Table(override val name: String) extends Label(name)
  case class Column(override val name: String) extends Label(name)

  // function or aggregate symbol labels
  case class FunctionLabel(override val name: String) extends Label(name)
  case class AggregateLabel(override val name: String) extends Label(name)

  def prettyQuery(q: Query): String =
    q match
      case SELECT(selection, from) =>
        s"SELECT ${prettyProjection(selection)} ${prettyQuery(from)}"
      case FROM(clauses) =>
        "FROM " + clauses
          .map((q, l) => s"(${prettyQuery(q)}) $l")
          .reduce(_ + ", " + _)
      case WHERE(query, condition) =>
        s"${prettyQuery(query)} WHERE ${prettyPredicate(condition)}"
      case EXCEPT(left, right) =>
        s"${prettyQuery(left)} EXCEPT ${prettyQuery(right)}"
      case UNIONALL(left, right) =>
        s"${prettyQuery(left)} UNION ALL ${prettyQuery(right)}"
      case DISTINCT(query) => s"DISTINCT ${prettyQuery(query)}"
      case TableRef(table) => table.toString()

  def prettyProjection(p: Projection): String =
    p match
      case Star                           => "*"
      case ColumnProjection(t, Star)      => s"$t.*"
      case ColumnProjection(t, c: Column) => s"$t.$c"
      case LabelledProjection(expression, label) =>
        s"${prettyExpression(expression)} AS $label"
      case CompoundProjection(ps) =>
        ps.map(prettyProjection(_)).reduce(_ + ", " + _)

  def prettyPredicate(p: Predicate): String =
    p match
      case TRUE          => "TRUE"
      case FALSE         => "FALSE"
      case NOT(inner)    => s"NOT (${prettyPredicate(inner)})"
      case EXISTS(inner) => s"EXISTS (${prettyQuery(inner)})"
      case AND(left, right) =>
        s"(${prettyPredicate(left)}) AND (${prettyPredicate(right)})"
      case OR(left, right) =>
        s"(${prettyPredicate(left)}) OR (${prettyPredicate(right)})"
      case EQ(left, right) =>
        s"(${prettyExpression(left)}) = (${prettyExpression(right)})"
      case LT(left, right) =>
        s"(${prettyExpression(left)}) < (${prettyExpression(right)})"
      case LE(left, right) =>
        s"(${prettyExpression(left)}) <= (${prettyExpression(right)})"

  def prettyExpression(e: Expression): String =
    e match
      case ColumnRef(t, c) => s"$t.$c"
      case Function(label, args) =>
        s"$label(${args.map(prettyExpression(_)).reduce(_ + ", " + _)})"
      case Aggregate(label, arg) => s"$label(${prettyQuery(arg)})"
      case Constant(value)       => value.toString()
}
