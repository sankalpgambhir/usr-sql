package usrsql

import usrsql.SQLLanguage.True
import usrsql.SQLLanguage.Eq

// ----------------------------------------------------
// ------------------- USR Language -------------------
object USRLanguage {

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
            val ses = es.map(sumPrefix(_)).filter(_ != One)
            val sum = ses.collectFirst {case s: Sum => s}

            if ses.isEmpty then
                One
            else if ses.contains(Zero) then
                Zero
            else if sum.isEmpty then
                U_x(ses)
            else
                Sum(sum.get.v, sumPrefix(sum.get.e * U_x(es.filterNot(_ == sum.get).map(e => locallyNameless(e, sum.get.v.name.toInt)))))

        case Squash(l) => Squash(sumPrefix(l))
        case Sum(v, e) => Sum(v, sumPrefix(e))
        case _ => e

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

def rankGen(classes: (Seq[UVar], Int)*): UValue => Int = 
    val cMap: Map[UVar, Int] = classes.foldLeft(Map.empty) {
        case (m, (s, i)) => m ++ s.map((_, i))
    }
    a =>
        a match
            case UInt(_) => 128 // eliminate the ref if you can
            case v@UVar(_) => cMap.getOrElse(v, 3)
            case Ref(t, _) => cMap.getOrElse(t, 2)
    

def eliminateUnusedVars(e: USRExpression, toKeep: Seq[UVar] = Seq.empty): Option[USRExpression] = 
    val used = collectUsedVars(e) ++ toKeep
    val gen = rankGen((used, 64), (toKeep, 32))
    val uf = UnionFind[UValue](Map.empty, Map.empty, gen)
    eliminateVars(e, used, used.foldLeft(uf)((u, e) => u + e))

def eliminateVars(e: USRExpression, toKeep: Seq[UVar], substs: UnionFind[UValue]): Option[USRExpression] =
    e match
        case Zero => Some(Zero)
        case One => Some(One)
        case UNot(l) => eliminateVars(l, toKeep, substs).map(UNot(_))
        case U_+(es) => 
            val ees = es.map(eliminateVars(_, toKeep, substs))
            if ees.exists(_.isEmpty) then None else Some(U_+(ees.map(_.get)))
        case U_x(es) => 
            val ees = es.map(exp =>
                val uf = es.foldLeft(substs) {
                    case (uf, e1) if e1 != exp =>
                        e1.match
                            case UEq(l, r) => uf.union(l, r)
                            case _ => uf
                    case (uf, _) => uf
                }
                eliminateVars(exp, toKeep, uf)
            )
            if ees.exists(_.isEmpty) then None else Some(U_x(ees.map(_.get)))
        case Squash(l) => eliminateVars(l, toKeep, substs).map(Squash(_))
        case Relation(r, v) => 
            substs.find(v) map {
                case v1: UVar => Relation(r, v1)
                case _ => Relation(r, v)
            }
        case Sum(v, e) =>
            val e1 = eliminateVars(e, toKeep, substs)
            e1.map { x =>
                if collectUsedVars(x).contains(v) then Sum(v, x) else x
            }
        case UEq(le, re) =>
            val lp = (substs + le).find(le).get
            val rp = (substs + re).find(re).get
            // if you can eliminate something entirely, do it
            // but what if t1 = t2 and t1.c = t3.c are known
            // and we see t1.c = t4.c
            // should it go to t2.c, or t3.c? 
            // if one or more of them are `toKeep`, it shouldn't matter too much
            // but if both of them are expected to be eliminated, it could cause insufficient reduction?
            // no, it shouldn't. `subst` contains the maximum amount of information available at this point
            // if `subst` and `toKeep` cannot eliminate something here, it should return `None`

            def canEliminate(s: UValue): Boolean = 
                s match
                    case v@UVar(name) => toKeep.contains(v)
                    case UInt(value) => true
                    case Ref(t, c) => toKeep.contains(t)

            if (lp == rp)
                Some(One) // equality can be eliminated
            else if (canEliminate(lp) && canEliminate(rp))
                Some(UEq(lp, rp))
            else
                None
        case ULeq(le, re) =>
            val lp = substs(le)
            val rp = substs(re)
            if (le == re) then 
                Some(One)
            else if !(toKeep.contains(lp) || toKeep.contains(rp)) then
                None
            else Some(ULeq(le, re))
        case ULt(le, re) =>
            val lp = substs(le)
            val rp = substs(re)
            if (le == re) then 
                Some(Zero)
            else if !(toKeep.contains(lp) || toKeep.contains(rp)) then
                None
            else Some(ULt(le, re))
    

// throw if an expression cannot be converted to SQL meaningfuly
object NonSQLExpressionException extends Exception()

}
