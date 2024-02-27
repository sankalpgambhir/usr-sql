package usrsql.language

import usrsql.Schema.DataType

object SQLIR:

  sealed trait Query

  case class SELECT(
    columns: Seq[Value],
    from: Seq[TableRef],
    where: Predicate,
    distinct: Boolean
  ) extends Query

  case class UNIONALL(left: Query, right: Query) extends Query
  case class EXCEPT(left: Query, right: Query) extends Query

  sealed trait TableRef:
    val source: Table | Query
    val name: String
  case class TableAlias(source: Table, name: String) extends TableRef
  case class QueryAlias(source: Query, name: String) extends TableRef

  case class Table(name: String)
  case class Column(name: String)

  sealed trait Predicate

  case object TRUE extends Predicate
  case object FALSE extends Predicate

  sealed trait UnaryConnector extends Predicate:
    val inner: Predicate
  
  case class NOT(inner: Predicate) extends UnaryConnector

  case class EXISTS(inner: Query) extends Predicate

  sealed trait BinaryConnector extends Predicate:
    val left: Predicate
    val right: Predicate

  case class AND(left: Predicate, right: Predicate) extends BinaryConnector  
  case class OR(left: Predicate, right: Predicate) extends BinaryConnector  

  sealed trait BinaryPredicate extends Predicate:
    val left: Value
    val right: Value

  case class EQ(left: Value, right: Value) extends BinaryPredicate
  case class LT(left: Value, right: Value) extends BinaryPredicate
  case class LE(left: Value, right: Value) extends BinaryPredicate

  sealed trait Value

  case class Constant(data: DataType) extends Value

  sealed trait Projection extends Value

  case class TableProjection(table: Table) extends Projection
  case class Left(inner: Projection) extends Projection
  case class Right(inner: Projection) extends Projection

end SQLIR

