package usrsql.language

import RelType.*

object SQL: 

  sealed trait Relational extends Typed
  sealed trait Query extends Relational
  case class LabelledRelational(label: Label, relational: Relational) extends Relational:
    require(label.tpe == relational.tpe)
    def tpe = relational.tpe
  
  case class Select(
    selectors: Seq[Selector],
    from: Seq[LabelledRelational],
    where: Predicate
  ) extends Query:
    def tpe = 
      selectors.map(_.tpe).reduceLeft(Node(_, _))

  case class Union(
    left: Query,
    right: Query
  ) extends Query:
    require(left.tpe == right.tpe)
    def tpe = left.tpe

  case class Join(
    left: Query,
    right: Query,
    on: Predicate
  ) extends Query:
    def tpe = Node(left.tpe, right.tpe)

  case class Distinct(inner: Query) extends Query:
    def tpe = inner.tpe

  sealed trait Selector extends Typed
  sealed trait Projection extends Selector
  case class LeftProjection(inner: Projection) extends Projection:
    def tpe = 
      inner.tpe match
        case Node(l, _) => l
        case _ => throw InvalidTypeException
  case class RightProjection(inner: Projection) extends Projection:
    def tpe = 
      inner.tpe match
        case Node(_, r) => r
        case _ => throw InvalidTypeException
  case class TableProjection(table: Table) extends Projection:
    def tpe = table.tpe

  case class ConstantSelector(value: Constant) extends Selector:
    def tpe = value.tpe
  case class FunctionalSelector(fun: Functional, args: Seq[Selector]) extends Selector:
    require(fun.itpe == args.map(_.tpe))
    def tpe = 
      fun.otpe

  sealed trait Table extends Relational
  case class TableReference(name: String, tpe: RelType) extends Table

  sealed trait Constant extends Typed
  case class BoxedConstant(value: Any, constType: BaseType) extends Constant:
    def tpe = Leaf(constType)

  case class Label(name: String, tpe: RelType) extends Typed

  case class Functional(name: String, itpe: Seq[RelType], otpe: RelType)

  sealed trait Predicate
  case class And(left: Predicate, right: Predicate) extends Predicate
  case class Or(left: Predicate, right: Predicate) extends Predicate
  case class Not(inner: Predicate) extends Predicate
  case class Eq(left: Selector, right: Selector) extends Predicate
  case class Gt(left: Selector, right: Selector) extends Predicate
  case class Lt(left: Selector, right: Selector) extends Predicate
  case class Uninterpreted(name: String, args: Seq[Selector]) extends Predicate

end SQL
