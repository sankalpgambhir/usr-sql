import usrsql.SQLLanguage.* 
import usrsql.USRLanguage.* 
import usrsql.UnionFind

Select(SelectType.DISTINCT, Star, TableRef("R", "t1"), INT(12) <= INT(14))

val uf = UnionFind[Int](Map.empty, Map.empty, x => 1)
(uf + 5 + 6).union(5, 6)(6)

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
            (UInt(12) === Ref(t1, "a"))
        ))

exp.toSumPrefix
eliminateUnusedVars(exp.toSumPrefix, Seq(t))

!Sum(t, (t === t1))

val exp1 = Sum(t, Sum(t1, Sum(t2, t === t1)) + Sum(t1, Sum(t2, t === t2)))
exp1.toLocallyNameless
exp1.toSumPrefix


val e2 = Sum(t1, (t === t1) * Relation("R", t1))
e2.toSumPrefix
eliminateUnusedVars(e2.toSumPrefix, Seq(UVar("t"))).get

// Query in SankalP Normal Form
// sum t0 sum t1 sum t2
    // x
    // FROM:
    //     R(t2)
    //     R(t1)
    // WHERE:
    //     t2.k = t0.k
    //     t2.a = t0.a
    //     12 <= t0.a
    //     t0.k = t1.k
    //     t0.c = t1.c
    //     t1 = t

// select * from R as t
// USR:
// \t -> sum_t1 R(t1) x t1 = t
// \t -> sum_t2 sum_t1 R(t1) x t1 = t
// \t -> (sum_t2 1) x sum_t1 ...
// \t ->

// SELECT t1.c AS a FROM t1 WHERE EXISTS(SELECT * FROM t1 where t1.c = a)

// EXISTS (SELECT * ...)
// NOT EXISTS (SELECT * ...)

// Sum t1 sum t2 ...  ++ Sum t3 ...
// ???
// (select ... from R as t1, R as t2) UNION (Select ... from R as t3)

// \t -> sum t1 (t1.a = 12) x R[t1]

// there must be exactly one free variable

// every summation variable must be bounded with a domain
// or eliminated

// something about maintaining the shape during a union?

// have manual examples for SQL queries in USR and we can go back

