package usrsql

object SQLLanguage{
    
sealed trait SQLExpression

enum SelectType:
    case DISTINCT, ALL

// --------------------------------------------------
sealed trait SelectExpression

case object Star extends SelectExpression {
    override def toString(): String = "*"
}
case class Columns(val columns: ColumnRef*) extends SelectExpression {
    override def toString(): String = columns.map(_.toString()).reduce(_ ++ ", " ++ _)
}

// --------------------------------------------------
sealed trait FromExpression

case class TableRef(val name: String, val original: Option[String]) extends FromExpression {
    override def toString(): String = if original.isDefined then s"(${original.get} AS $name)" else name
}
object TableRef {
    def apply(o: String, n: String): TableRef = TableRef(n, Some(o))
    def apply(n: String): TableRef = TableRef(n, None)
}

// or a SELECT
case class Join(val queries: FromExpression*) extends FromExpression {
    override def toString(): String = queries.map(_.toString()).reduce(_ ++ ", " ++ _)
}

// --------------------------------------------------
sealed trait Value

case class ColumnRef(val table: TableRef, val name: String) extends Value { 
    // possibly t1.* as well
    override def toString(): String = s"$table.$name"
}
case class INT(val value: Int) extends Value

sealed trait Expression

// propositional
case object False extends Expression
case object True extends Expression

case class Not(val l: Expression) extends Expression {
    override def toString(): String = s"!$l"
}
case class And(val l: Expression, val r: Expression) extends Expression {
    override def toString(): String = s"($l /\\ $r)"
}
case class Or(val l: Expression, val r: Expression) extends Expression {
    override def toString(): String = s"($l \\/ $r)"
}

case class Is(val l: Expression, val r: Expression) extends Expression {
    override def toString(): String = s"($l is $r)"
}

extension (e: Expression) {
    def unary_! = Not(e)
    infix def /\(f: Expression) = And(e, f) 
    infix def \/(f: Expression) = Or(e, f)
    infix def is(f: Expression) = Is(e, f)
}

// mathematical
case class Eq(val l: Value, val r: Value) extends Expression {
    override def toString(): String = s"($l = $r)"
}
case class Leq(val l: Value, val r: Value) extends Expression {
    override def toString(): String = s"($l <= $r)"
}
case class Lt(val l: Value, val r: Value) extends Expression {
    override def toString(): String = s"($l < $r)"
}

extension (v: Value) {
    infix def ===(w: Value) = Eq(v, w)
    infix def <=(w: Value) = Leq(v, w)
    infix def <(w: Value) = Lt(v, w)
}

case class Select(
    val selectType: SelectType,
    val selectExpression: SelectExpression,
    val from: FromExpression,
    val where: Expression,
    val groupBy: Expression = True
) extends SQLExpression with FromExpression {
    override def toString(): String =
        s"SELECT ${if selectType == SelectType.DISTINCT then "DISTINCT " else ""}" +
          s"$selectExpression FROM $from WHERE $where${if groupBy == True then "" else s" GROUP BY $groupBy"}"
}
}
