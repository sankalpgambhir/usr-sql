package usrsql.language

import usrsql.Schema.DataType

object SQL:

  sealed trait Query:
    override def toString(): String = 
      this match
        case SELECT(columns, from, where, distinct) =>
          val distinctStr = if distinct then "DISTINCT " else ""
          val columnsStr = columns.map(_.toString()).mkString(", ")
          val fromStr = from.map(_.toString()).mkString(", ")
          val whereStr = where.toString()
          s"SELECT $distinctStr$columnsStr FROM $fromStr WHERE $whereStr"
        case UNIONALL(left, right) =>
          s"(${left.toString()}) UNION ALL (${right.toString()})"
        case EXCEPT(left, right) =>
          s"(${left.toString()}) EXCEPT (${right.toString()})"

  case class SELECT(
    val columns: Seq[Value],
    val from: Seq[TableRef],
    val where: Predicate,
    val distinct: Boolean
  ) extends Query

  case class UNIONALL(left: Query, right: Query) extends Query
  case class EXCEPT(left: Query, right: Query) extends Query

  sealed trait TableRef
  case class TableAlias(source: Table, name: String) extends TableRef:
    override def toString(): String = s"$source AS $name"
  case class QueryAlias(source: Query, name: String) extends TableRef:
    override def toString(): String = s"($source) AS $name"

  case class Table(name: String):
    override def toString(): String = name
  case class Column(name: String):
    override def toString(): String = name

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

  case class ColumnRef(table: Table, column: Column) extends Value:
    override def toString(): String = s"$table.$column"
  case class Constant(data: DataType) extends Value

end SQL

