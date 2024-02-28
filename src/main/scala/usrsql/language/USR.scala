package usrsql.language

import RelType.*

object USR:

    case class Label(name: String, tpe: Option[RelType])

    sealed trait Expr:
        def children: Seq[Expr]
    case class Var(name: String, tpe: RelType) extends Expr with Typed:
        def children = Seq.empty
    case class BoxedConst(value: Any, tpe: RelType) extends Expr with Typed:
        def children = Seq.empty

    // SR base ops
    case class Mul(left: Expr, right: Expr) extends Expr:
        def children = Seq(left, right)
    case class Add(left: Expr, right: Expr) extends Expr:
        def children = Seq(left, right)
    case class Not(inner: Expr) extends Expr:
        def children = Seq(inner)

    // USR specific ops
    case class Squash(inner: Expr) extends Expr:
        def children = Seq(inner)
    case class USum(variable: Var, inner: Expr) extends Expr:
        def children = Seq.empty

    // lift to lambdas
    case class Lambda(variable: Var, inner: Expr) extends Expr:
        def eval(arg: Expr): Expr = inner.substituted(Map(variable -> arg))
        def children = Seq(inner)

    case class App(fun: Lambda, arg: Expr) extends Expr:
        def eval: Expr = fun.eval(arg)
        def children = Seq(fun, arg)

    extension (e: Expr) def substituted(subst: Map[Var, Expr]): Expr =
        e match
            case v @ Var(name, tpe) => subst.getOrElse(v, e)
            case BoxedConst(_, _) => e
            case Mul(left, right) => Mul(left.substituted(subst), right.substituted(subst))
            case Add(left, right) => Add(left.substituted(subst), right.substituted(subst))
            case Not(inner) => Not(inner.substituted(subst))
            case Squash(inner) => Squash(inner.substituted(subst))
            case USum(variable, inner) => USum(variable, inner.substituted(subst - variable))
            case App(Lambda(v, body), arg) => App(Lambda(v, body.substituted(subst - v)), arg.substituted(subst))
            case Lambda(variable, inner) => Lambda(variable, inner.substituted(subst - variable))
