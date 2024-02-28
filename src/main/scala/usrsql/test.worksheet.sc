
import usrsql.language.RelType.*
import usrsql.language.USR.*
import usrsql.rewrite.EGraphs.*

val term1 = 
    App(
        Lambda(
            Var("x", Leaf(IntType)), 
            Add(Var("x", Leaf(IntType)), BoxedConst(1, Leaf(IntType)))
        ), 
        BoxedConst(2, Leaf(IntType))
    )

fromExpr(term1).generate.toList

