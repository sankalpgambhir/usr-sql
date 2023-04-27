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
    override def toString(): String = s"$t.$name"
}
case class INT(val value: Int) extends Value

// --------------------------------------------------
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

Select(SelectType.DISTINCT, Star, TableRef("R", "t1"), INT(12) <= INT(14))


// ----------------------------------------------------
// ------------------- USR Language -------------------

sealed trait USRExpression

sealed trait UValue

case class UVar(val name: String) extends UValue {
    override def toString(): String = name
}
case class UInt(val value: Int) extends UValue {
    override def toString(): String = value.toString()
}
case class Ref(val t: UVar, val c: String) extends UValue {
    override def toString(): String = s"$t.$c"
}

// semiring
case object Zero extends USRExpression
case object One extends USRExpression
case class UNot(val l: USRExpression) extends USRExpression {
    override def toString(): String = s"not($l)"
}
case class U_+(val es: Seq[USRExpression]) extends USRExpression {
    override def toString(): String = "(" ++ es.map(_.toString()).reduce(_ ++ " + " ++ _) ++ ")"
}
case class U_x(val es: Seq[USRExpression]) extends USRExpression {
    override def toString(): String = "(" ++ es.map(_.toString()).reduce(_ ++ " x " ++ _) ++ ")"
}

case class Squash(val l: USRExpression) extends USRExpression {
    override def toString(): String = s"||$l||"
}
case class Relation(val r: String, val v: UVar) extends USRExpression {
    override def toString(): String = s"$r[$v]"
}

// sum
case class Sum(val v: UVar, val e: USRExpression) extends USRExpression {
    override def toString(): String = s"Σ_$v ($e)"
}

// mathematical
sealed trait BinaryComparisonUSR(val l: UValue, val r: UValue) extends USRExpression
case class UEq(val le: UValue, val re: UValue) extends BinaryComparisonUSR(le, re) {
    override def toString(): String = s"($l = $r)"
}
case class ULeq(val le: UValue, val re: UValue) extends BinaryComparisonUSR(le, re) {
    override def toString(): String = s"($l <= $r)"
}
case class ULt(val le: UValue, val re: UValue) extends BinaryComparisonUSR(le, re) {
    override def toString(): String = s"($l < $r)"
}

extension (v: UValue) {
    infix def ===(w: UValue) = UEq(v, w)
    infix def <=(w: UValue) = ULeq(v, w)
    infix def <(w: UValue) = ULt(v, w)
}

extension (e: USRExpression) {
    def unary_! = UNot(e)
    infix def +(f: USRExpression) = 
        (e, f) match
            case (U_+(es), U_+(fs)) => U_+((es ++ fs))
            case (_, U_+(fs)) => U_+((e +: fs))
            case (U_+(es), f) => U_+((f +: es))
            case _ => U_+(Seq(e, f))
        
    infix def *(f: USRExpression) = 
        (e, f) match
            case (U_x(es), U_x(fs)) => U_x((es ++ fs))
            case (_, U_x(fs)) => U_x((e +: fs))
            case (U_x(es), f) => U_x((f +: es))
            case _ => U_x(Seq(e, f))

    def toLocallyNameless = locallyNameless(e)
    def toSumPrefix = sumPrefix(e.toLocallyNameless)
    def substituted(substs: (UVar, UVar)*) = substitute(e, substs.toMap)
}

// def produceRewrites(e: USRExpression): LazyList[USRExpression] = 
//     e match
//         case Zero => Zero
//         case One => One
//         case UNot(l) =>
//         case U_+(l, r) =>
//         case U_x(l, r) =>
//         case Squash(l) =>
//         case Sum(v, e) =>

def substitute(e: USRExpression, substs: Map[UVar, UVar]): USRExpression =
    e match
        case UNot(l) => UNot(substitute(e, substs))
        case U_+(es) => U_+(es.map(substitute(_, substs)))
        case U_x(es) => U_x(es.map(substitute(_, substs)))
        case Squash(l) => Squash(substitute(l, substs))
        case Sum(v, l) => Sum(v, substitute(l, substs - v))
        case UEq(l, r) => UEq(substitute(l, substs), substitute(r, substs))
        case ULeq(l, r) => ULeq(substitute(l, substs), substitute(r, substs))
        case ULt(l, r) => ULt(substitute(l, substs), substitute(r, substs))
        case Relation(r, v) => Relation(r, substitute(v, substs).asInstanceOf[UVar])
        case _ => e

def substitute(v: UValue, substs: Map[UVar, UVar]): UValue = 
    v match
        case w: UVar if substs.get(w).isDefined => substs(w)
        case Ref(t, c) => Ref(substitute(t, substs).asInstanceOf[UVar], c)
        case _ => v
    

def locallyNameless(e: USRExpression, counter: Int = 0): USRExpression = 
    e match
        case UNot(l) => UNot(locallyNameless(l, counter))
        case U_+(es) => U_+(es.map(locallyNameless(_, counter)))
        case U_x(es) => U_x(es.map(locallyNameless(_, counter)))
        case Squash(l) => Squash(locallyNameless(l, counter))
        case Sum(v, l) => {
            val u = UVar(counter.toString())
            Sum(u, locallyNameless(l.substituted(v -> u), counter + 1))
        }
        case _ => e

def sumPrefix(e: USRExpression): USRExpression = 
    // require locally nameless e
    e match
        case Zero => Zero
        case One => One
        case UNot(l) => 
            sumPrefix(l) match
                case Zero => One
                case UNot(l) => sumPrefix(l)
                case U_+(es) => sumPrefix(U_+(es.map(!_)))
                case U_x(es) => sumPrefix(U_x(es.map(!_)))
                case Squash(l) => sumPrefix(!l)
                case f => !f

        case U_+(es) =>
            val ses = es.map(sumPrefix(_))
            val sums = ses.collect {case s: Sum => s}
            lazy val notsums = ses.filter {case s: Sum => false case _ => true} 

            if sums.isEmpty then
                U_+(ses)
            else
                Sum(sums.head.v, U_+(sums.map(_.e))) + U_+(notsums)

        case U_x(es) =>
            val ses = es.map(sumPrefix(_))
            val sum = ses.collectFirst {case s: Sum => s}

            if sum.isEmpty then
                U_x(ses)
            else
                Sum(sum.get.v, sumPrefix(sum.get.e * U_x(es.filterNot(_ == sum.get).map(e => locallyNameless(e, sum.get.v.name.toInt)))))

        case Squash(l) => Squash(sumPrefix(l))
        case Sum(v, e) => Sum(v, sumPrefix(e))
        case _ => e

val t = UVar("t")
val t1 = UVar("t1")
val t2 = UVar("t2")
val t3 = UVar("t3")

val exp = Sum(t1, Sum(t2, (t2 === t) * 
            Sum(t3, 
                (Ref(t3, "k") === Ref(t1, "k")) * 
                (Ref(t3, "a") === Ref(t1, "a")) *
                Relation("R", t3)
            ) *
            Relation("R", t2) *
            (Ref(t1, "k") === Ref(t2, "k")) *
            (UInt(12) <= Ref(t1, "a"))
        ))

exp.toSumPrefix

// throw if an expression cannot be converted to SQL meaningfuly
object NonSQLExpressionException extends Exception()

def collectUsedVars(e: USRExpression): Seq[UVar] = 
    e match
        case Zero => Seq.empty
        case One => Seq.empty
        case UNot(l) => collectUsedVars(l)
        case U_+(es) => es.flatMap(collectUsedVars(_)).distinct
        case U_x(es) => es.flatMap(collectUsedVars(_)).distinct
        case Squash(l) => collectUsedVars(l)
        case Relation(r, v) => Seq(v)
        case Sum(v, e) => collectUsedVars(e)
        case ec: BinaryComparisonUSR => Seq.empty
            
    

def toSQL(e: USRExpression): SQLExpression =
    val simp = e.toSumPrefix
    
    val usedVars = collectUsedVars(e)

    ???


collectUsedVars(exp.toSumPrefix)
// toSQL(exp)