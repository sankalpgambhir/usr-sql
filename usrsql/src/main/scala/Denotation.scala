package usrsql

object DenotationSQLToIR {

  /** Translating SQL to SQLIR
    */
  def sqlToIR(exp: SQL.Query, schema: Schema.Linear): SQLIR.Query =
    // compute column indices
    val columnIndices: (SQL.Table, SQL.Column) => SQLIR.Projection = {
      case (SQL.Table(tn), SQL.Column(cn)) =>
        val index = schema.tables(tn).unzip._1.indexOf(cn)
        val max = schema.tables(tn).length - 1
        val tableProj = SQLIR.TableProjection(SQLIR.Table(tn))
        // add n-1 Right moves
        val node =
          (0 until index).foldLeft(tableProj: SQLIR.Projection)((t, _) =>
            SQLIR.RightProjection(t)
          )
        // add a final move
        if index == max then SQLIR.RightProjection(node)
        else SQLIR.LeftProjection(node)
    }

    // TODO: resolve table aliases?

    sqlQueryToIR(exp, columnIndices)

  private def sqlQueryToIR(
      exp: SQL.Query,
      columnIndices: (SQL.Table, SQL.Column) => SQLIR.Projection
  ): SQLIR.Query =
    exp match
      case SQL.SELECT(selection, from) =>
        SQLIR.SELECT(
          sqlProjectionToIR(selection, columnIndices),
          sqlQueryToIR(from, columnIndices)
        )
      case SQL.FROM(clauses) =>
        SQLIR.FROM(clauses.map { case (q, SQL.QueryLabel(name)) =>
          (sqlQueryToIR(q, columnIndices), SQLIR.QueryLabel(name))
        })
      case SQL.WHERE(query, condition) =>
        SQLIR.WHERE(
          sqlQueryToIR(query, columnIndices),
          sqlPredicateToIR(condition, columnIndices)
        )
      case SQL.EXCEPT(left, right) =>
        SQLIR.EXCEPT(
          sqlQueryToIR(left, columnIndices),
          sqlQueryToIR(right, columnIndices)
        )
      case SQL.UNIONALL(left, right) =>
        SQLIR.UNIONALL(
          sqlQueryToIR(left, columnIndices),
          sqlQueryToIR(right, columnIndices)
        )
      case SQL.DISTINCT(query) =>
        SQLIR.DISTINCT(sqlQueryToIR(query, columnIndices))
      case SQL.TableRef(SQL.Table(name)) => SQLIR.TableRef(SQLIR.Table(name))

  private def sqlProjectionToIR(
      p: SQL.Projection,
      columnIndices: (SQL.Table, SQL.Column) => SQLIR.Projection
  ): SQLIR.Projection =
    p match
      case SQL.Star => SQLIR.Star
      case SQL.ColumnProjection(t, SQL.Star) =>
        ??? // TODO: t.*? should just become t?
      case SQL.ColumnProjection(t, c: SQL.Column) => columnIndices(t, c)
      case SQL.LabelledProjection(expression, label) =>
        SQLIR.ExpressionProjection(
          sqlExpressionToIR(expression, columnIndices)
        ) // throw away the label
      case SQL.CompoundProjection(ps) =>
        SQLIR.CompoundProjection(ps.map(sqlProjectionToIR(_, columnIndices)))

  private def sqlPredicateToIR(
      p: SQL.Predicate,
      columnIndices: (SQL.Table, SQL.Column) => SQLIR.Projection
  ): SQLIR.Predicate =
    p match
      case SQL.TRUE       => SQLIR.TRUE
      case SQL.FALSE      => SQLIR.FALSE
      case SQL.NOT(inner) => SQLIR.NOT(sqlPredicateToIR(inner, columnIndices))
      case SQL.EXISTS(inner) => SQLIR.EXISTS(sqlQueryToIR(inner, columnIndices))
      case SQL.AND(left, right) =>
        SQLIR.AND(
          sqlPredicateToIR(left, columnIndices),
          sqlPredicateToIR(right, columnIndices)
        )
      case SQL.OR(left, right) =>
        SQLIR.OR(
          sqlPredicateToIR(left, columnIndices),
          sqlPredicateToIR(right, columnIndices)
        )
      case SQL.EQ(left, right) =>
        SQLIR.EQ(
          sqlExpressionToIR(left, columnIndices),
          sqlExpressionToIR(right, columnIndices)
        )
      case SQL.LT(left, right) =>
        SQLIR.LT(
          sqlExpressionToIR(left, columnIndices),
          sqlExpressionToIR(right, columnIndices)
        )
      case SQL.LE(left, right) =>
        SQLIR.LE(
          sqlExpressionToIR(left, columnIndices),
          sqlExpressionToIR(right, columnIndices)
        )

  private def sqlExpressionToIR(
      e: SQL.Expression,
      columnIndices: (SQL.Table, SQL.Column) => SQLIR.Projection
  ): SQLIR.Expression =
    e match
      case SQL.ColumnRef(t, c) =>
        SQLIR.ProjectionExpression(columnIndices(t, c))
      case SQL.Function(SQL.FunctionLabel(symbol), args) =>
        SQLIR.Function(
          SQLIR.FunctionLabel(symbol),
          args.map(sqlExpressionToIR(_, columnIndices))
        )
      case SQL.Aggregate(SQL.AggregateLabel(symbol), arg) =>
        SQLIR.Aggregate(
          SQLIR.AggregateLabel(symbol),
          sqlQueryToIR(arg, columnIndices)
        )
      case SQL.Constant(value) => SQLIR.Constant(value)
}

object TestSQLToIR {
  import SQL.*
  val testQuery =
    WHERE(
      SELECT(Star, FROM(Seq(TableRef(Table("t1")) AS QueryLabel("t10")))),
      EQ(ColumnRef(Table("t1"), Column("c")), Constant(Schema.DataType.Int(5)))
    )
  val testQuery2 =
    WHERE(
      SELECT(
        ColumnProjection(Table("t1"), Column("d")),
        FROM(Seq(TableRef(Table("t1")) AS QueryLabel("t10")))
      ),
      EQ(ColumnRef(Table("t1"), Column("c")), Constant(Schema.DataType.Int(5)))
    )

  val testSchema =
    Schema.Linear(
      Map(
        "t1" -> Seq(
          "c" -> Schema.DataType.Int(5),
          "d" -> Schema.DataType.Int(5)
        )
      )
    )

  @main def testDenotation =
    println(testQuery)
    println(DenotationSQLToIR.sqlToIR(testQuery, testSchema))
    println(testQuery2)
    println(DenotationSQLToIR.sqlToIR(testQuery2, testSchema))
}

object DenotationIRToUSR {

  import SQLIR.*

  object MalformedSQLExpressionException extends Exception()
  object ImpossibleProjectionDenotation extends Exception()

  val queryVar = USR.Variable("t")

  def tableToVar(t: Table): USR.Variable = USR.Variable(t.name)

  def tableToRelation(t: Table): USR.Relation = USR.Relation(t.name)

  def irQueryToUSR(q: Query): USR.Expression =
    q match
      case s: SELECT => generateSelect(s)
      case FROM(_)   =>
        // shouldn't see a top level FROM
        throw MalformedSQLExpressionException
      case WHERE(query, condition) =>
        query match
          case s: SELECT =>
            generateSelect(s, addedConstraints = irPredicateToUSR(condition))
          case _ => throw MalformedSQLExpressionException
      case EXCEPT(left, right) =>
        irQueryToUSR(left) * USR.Not(irQueryToUSR(right))
      case UNIONALL(left, right) =>
        irQueryToUSR(left) + irQueryToUSR(right)
      case DISTINCT(query) =>
        query match
          case s: SELECT => USR.Squash(irQueryToUSR(s))
          case _ =>
            throw MalformedSQLExpressionException // DISTINCT can only contain SELECT
      case TableRef(table) =>
        // if input query is the table R
        // return \t -> R[t]
        USR.RelationRef(tableToRelation(table), queryVar)

  def irPredicateToUSR(p: Predicate): USR.Expression =
    p match
      case TRUE          => USR.One
      case FALSE         => USR.Zero
      case NOT(inner)    => USR.Not(irPredicateToUSR(inner))
      case EXISTS(inner) => USR.Squash(USR.USum(queryVar, irQueryToUSR(inner)))
      case AND(left, right) => irPredicateToUSR(left) * irPredicateToUSR(right)
      case OR(left, right) =>
        USR.Squash(irPredicateToUSR(left) + irPredicateToUSR(right))
      case EQ(left, right) =>
        USR.EQ(irExpressionToUSR(left), irExpressionToUSR(right))
      case LT(left, right) =>
        USR.LT(irExpressionToUSR(left), irExpressionToUSR(right))
      case LE(left, right) =>
        USR.LE(irExpressionToUSR(left), irExpressionToUSR(right))

  def irExpressionToUSR(e: Expression): USR.Value =
    e match
      case ProjectionExpression(p) => irProjectionToValue(p)
      case Function(FunctionLabel(name), args) =>
        USR.Function(USR.FunctionLabel(name), args.map(irExpressionToUSR(_)))
      case Aggregate(AggregateLabel(name), arg) =>
        USR.Aggregate(USR.AggregateLabel(name), irQueryToUSR(arg))
      case Constant(value) => USR.Constant(value)

  def irProjectionToValue(p: Projection): USR.Value =
    p match
      case Star                      => throw ImpossibleProjectionDenotation
      case TableProjection(t)        => tableToVar(t)
      case LeftProjection(of)        => USR.Left(irProjectionToValue(of))
      case RightProjection(of)       => USR.Right(irProjectionToValue(of))
      case CompoundProjection(ps)    => throw ImpossibleProjectionDenotation
      case ExpressionProjection(exp) => irExpressionToUSR(exp)

  def generateSelect(
      s: SELECT,
      addedConstraints: USR.Expression = USR.One
  ): USR.Expression =
    // for each join clause,
    // define a variable
    // if the join clause is a relation, done
    // if it is a query, add its constraints

    ???

}
