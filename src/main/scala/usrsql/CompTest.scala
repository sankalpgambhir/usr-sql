package usrsql

import language.SQL.*
import denotation.{ToIR => dti, ToUSR => dtu}
import compilation.{ToIR => cti, ToSQL => cts}

object CompTest:
  val baseQuery = 
    SELECT(
      Seq(
        ColumnRef(Table("table1"), Column("c2"))
      ),
      Seq(
        TableAlias(Table("R"), "table1")
      ),
      TRUE,
      true
    )
  val schema = dti.SQLContext(Map("table1" -> Seq("c1", "c2")))
  val tctx = dti.TranslationContext(schema)
  val irquery = dti.translate(using tctx)(baseQuery)
  val usrquery = dtu.translate(irquery)

  val backir = cti.translate(usrquery)
  val backtctx = cts.TranslationContext(cts.SQLContext(schema.schema))
  val backsql = cts.translate(using backtctx)(backir)

  @main def testComp =
    println(baseQuery)
    println(irquery)
    println(usrquery)
    println(backir)
    println(backsql)
end CompTest
