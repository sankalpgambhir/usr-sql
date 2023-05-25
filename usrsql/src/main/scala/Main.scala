package usrsql

import usrsql.UnionFind
import usrsql.SQLLanguage.* 
import usrsql.USRLanguage.*

object ExpTest extends App {
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

  val e2 = Sum(t1, (t === t1) * Relation("R", t1))
  val e3 = eliminateUnusedVars(e2.toSumPrefix, Seq(t)).get
  collectUsedVars(e3)
}

