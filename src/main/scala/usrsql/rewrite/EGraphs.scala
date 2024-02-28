package usrsql.rewrite

import usrsql.language.USR.*
import scala.collection.mutable.{Set => MSet}
import usrsql.language.RelType.*


object EGraphs:
    enum USRLabel:
        case VarLabel(name: String, tpe: RelType)
        case BoxedConstLabel(value: Any, tpe: RelType)
        case MulLabel
        case AddLabel
        case NotLabel
        case SquashLabel
        case USumLabel
        case LambdaLabel
        case AppLabel

    import USRLabel.*

    def toLabel(expr: Expr): USRLabel = 
        expr match
            case Var(name, tpe) => VarLabel(name, tpe)
            case BoxedConst(value, tpe) => BoxedConstLabel(value, tpe)
            case Mul(_, _) => MulLabel
            case Add(_, _) => AddLabel
            case Not(_) => NotLabel
            case Squash(_) => SquashLabel
            case USum(_, _) => USumLabel
            case Lambda(_, _) => LambdaLabel
            case App(_, _) => AppLabel

    case class IllegalEGraphException(message: String) extends Exception(message)
    case object CannotInsertException extends Exception

    case class ENode(id: Int, label: USRLabel, children: Seq[EGraph]):
        def has(expr: Expr): Boolean = 
            val topLabel = toLabel(expr)
            label == topLabel && (expr.children zip children).forall((c, eg) => eg.find(c).isDefined)

    sealed trait EGraph
    case class EClass(id: Int, nodes: MSet[ENode]) extends EGraph
    case class EAbs(id: Int, label: USRLabel, variable: Var, inner: EGraph) extends EGraph

    object EClass:
        def empty = EClass(freshId(), MSet.empty)

    private var root = EClass.empty

    private var nextId = 0
    def freshId(): Int = 
        val id = nextId
        nextId += 1
        id

    def fromExpr(e: Expr): EGraph = 
        root.insert(e)

    extension (eg: EGraph) def generate: LazyList[Expr] =
        eg match
            case EClass(id, nodes) => nodes.to(LazyList).flatMap(_.generate)
            case EAbs(id, label, variable, inner) => inner.generate.map(Lambda(variable, _))

    extension (en: ENode) def generate: LazyList[Expr] =
        en match
            case ENode(_, VarLabel(name, tpe), _) => LazyList(Var(name, tpe))
            case ENode(_, BoxedConstLabel(value, tpe), _) => LazyList(BoxedConst(value, tpe))
            case ENode(_, MulLabel, Seq(left, right)) => 
                for
                    l <- left.generate
                    r <- right.generate
                yield Mul(l, r)
            case ENode(_, AddLabel, Seq(left, right)) => 
                for
                    l <- left.generate
                    r <- right.generate
                yield Add(l, r)
            case ENode(_, NotLabel, Seq(inner)) => inner.generate.map(Not.apply)
            case ENode(_, SquashLabel, Seq(inner)) => inner.generate.map(Squash.apply)
            case ENode(_, USumLabel, Seq(variable, inner)) => throw IllegalEGraphException("USum should not be in ENode")
            case ENode(_, LambdaLabel, Seq(variable, inner)) => throw IllegalEGraphException("Lambda should not be in ENode")
            case ENode(_, AppLabel, Seq(fun, arg)) => 
                for
                    f <- fun.generate
                    a <- arg.generate
                yield App(f.asInstanceOf[Lambda], a)
            case _ => 
                throw IllegalEGraphException(s"Found term $en")

    extension (eg: EGraph) def find(expr: Expr): Option[EGraph] =
        val topLabel = toLabel(expr)
        eg match
            case EClass(id, nodes) =>
                if nodes.exists(_.has(expr)) then Some(eg)
                else None
            case EAbs(id, label, variable, inner) if label == topLabel =>
                expr match
                    case USum(v, inn) =>
                        inner.find(inn.substituted(Map(v -> variable)))              
                    case Lambda(v, inn) =>
                        inner.find(inn.substituted(Map(v -> variable)))              
                    case _ => None
                
            case _ => None
        

    extension (eg: EGraph) def insert(expr: Expr): EGraph =
        doInsert(eg, expr)
        eg

    private def singleEGraph(expr: Expr): EGraph =
        expr match
            case USum(variable, inner) =>
                val innC = root.find(inner).getOrElse(singleEGraph(inner))
                EAbs(freshId(), USumLabel, variable, innC)
            case Lambda(variable, inner) =>
                val innC = root.find(inner).getOrElse(singleEGraph(inner))
                EAbs(freshId(), LambdaLabel, variable, innC)
            case _ => 
                val ec = EClass.empty
                ec.insert(expr)
                ec        
        

    private def doInsert(eg: EGraph, expr: Expr): Unit =
        val topLabel = toLabel(expr)
        if eg.find(expr).isDefined then 
            ()
        else
            eg match
                case EAbs(id, label, variable, inner) if label == topLabel =>
                    expr match
                        case USum(exprVar, inn) =>
                            doInsert(inner, inn.substituted(Map(exprVar -> variable)))
                        case Lambda(exprVar, inn) =>
                            doInsert(inner, inn.substituted(Map(exprVar -> variable)))
                        case _ => throw CannotInsertException
                case EClass(id, nodes) => 
                    val newClasses = expr.children.map(c => root.find(c).getOrElse(singleEGraph(c)))
                    val newNode = ENode(freshId(), topLabel, newClasses)
                    nodes += newNode
                case _ => throw CannotInsertException

    private def findMatch(eg: EGraph, expr: Expr): Option[Map[Var, Expr]] =
        ???
        
    private def doRewrite(eg: EGraph, rule: (Expr, Expr)): Boolean =
        val (lhs, rhs) = rule
        val matches = findMatch(eg, lhs)
        if matches.isDefined then {eg.insert(rhs.substituted(matches.get)); true} else false

    def rewriteOnce(rule: (Expr, Expr)): Unit =
        doRewrite(root, rule)

end EGraphs
