package usrsql

import usrsql.SQL.*

trait Injection[A, B, Context]:
  def translate(using ctx: Context)(source: A): B

object Context:
  type TableName = String
  type ColumnName = String

  enum Tree[A]:
    case Leaf(val value: A)
    case Fork(val left: Tree[A], val right: Tree[A])

  trait SQLContext:

    val schema: Map[TableName, List[ColumnName]]
  end SQLContext

  trait IRContext:

    val innerContext: SQLContext
    val schema: Map[TableName, Tree[ColumnName]]

    def fromColumn(tc: ColumnRef): SQLIR.Projection =
      val tableName = tc.t.name
      val columnName = tc.c.name
      findColumnInTree(SQLIR.Table(tableName), columnName, schema(tableName)).get

    def findColumnInTree(table: SQLIR.Table, columnName: ColumnName, tree: Tree[ColumnName]): Option[SQLIR.Projection] =
      tree match
        case Tree.Leaf(value) => 
          if value == columnName then
            Some(SQLIR.TableProjection(table))
          else
            None
        case Tree.Fork(left, right) =>
          findColumnInTree(table, columnName, left)
          .map(
            SQLIR.LeftProjection(_)
          )
          .orElse(
            findColumnInTree(table, columnName, right)
            .map(
              SQLIR.RightProjection(_)
            )
          )
      
  end IRContext

  object IRContext:
    private def toTree[A](l: Seq[A]): Tree[A] =
      require(l.length >= 1)
      if l.length == 1 then
        Tree.Leaf(l.head)
      else
        val halves = l.splitAt(l.length / 2)
        Tree.Fork(toTree(halves._1), toTree(halves._2))

    def fromSQLContext(ctx: SQLContext, query: Query): IRContext =
      def computeQuerySchema(query: SELECT): Seq[ColumnName] =
        query.from.clauses.map(_._2.name) // stupid

      // schema goes from being over "tables" to "table variables/labels"
      def computeVarSchema(query: Query): Map[TableName, Seq[ColumnName]] = 
        ctx.schema // TODO: Fix      

      val newSchema = computeVarSchema(query).map((t, l) => t -> toTree(l))
      new IRContext {val innerContext = ctx; val schema = newSchema}
  end IRContext
end Context

object SQLToIR extends Injection[SQL.Query, SQLIR.Query, Context.SQLContext]:
  import Context.IRContext
  import Context.SQLContext

  def translate(using ctx: SQLContext)(source: Query): SQLIR.Query = 
    queryToIR(using IRContext.fromSQLContext(ctx, source))(source)

  private def queryToIR(using ctx: IRContext)(source: Query): SQLIR.Query = 
    source match
      case SELECT(selection, from) => 
        SQLIR.SELECT(projectionToIR(selection), fromToIR(from))
      case FROM(clauses) =>
        fromToIR(FROM(clauses))
      case WHERE(query, condition) =>
        SQLIR.WHERE(queryToIR(query), predicateToIR(condition))
      case SQL.EXCEPT(left, right) =>
        SQLIR.EXCEPT(
          queryToIR(left),
          queryToIR(right)
        )
      case SQL.UNIONALL(left, right) =>
        SQLIR.UNIONALL(
          queryToIR(left),
          queryToIR(right)
        )
      case SQL.DISTINCT(query) =>
        SQLIR.DISTINCT(queryToIR(query))
      case SQL.TableRef(SQL.Table(name)) => SQLIR.TableRef(SQLIR.Table(name))

  private def fromToIR(using ctx: IRContext)(
    f: SQL.FROM
  ): SQLIR.FROM =
    SQLIR.FROM(f.clauses.map { case (q, SQL.QueryLabel(name)) =>
          (queryToIR(q), SQLIR.QueryLabel(name))
        })

  private def projectionToIR(using ctx: IRContext)(
      p: SQL.Projection
  ): SQLIR.Projection =
    p match
      case SQL.Star => SQLIR.Star
      case SQL.ColumnProjection(t, SQL.Star) =>
        ??? // TODO: t.*? should just become t?
      case SQL.ColumnProjection(t, c: SQL.Column) => 
        ctx.fromColumn(ColumnRef(t, c))
      case SQL.LabelledProjection(expression, label) =>
        SQLIR.ExpressionProjection(
          expressionToIR(expression)
        ) // throw away the label
      case SQL.CompoundProjection(ps) =>
        SQLIR.CompoundProjection(ps.map(projectionToIR(_)))

  private def predicateToIR(using ctx: IRContext)(
      p: SQL.Predicate
  ): SQLIR.Predicate =
    p match
      case SQL.TRUE       => SQLIR.TRUE
      case SQL.FALSE      => SQLIR.FALSE
      case SQL.NOT(inner) => SQLIR.NOT(predicateToIR(inner))
      case SQL.EXISTS(inner) => SQLIR.EXISTS(queryToIR(inner))
      case SQL.AND(left, right) =>
        SQLIR.AND(
          predicateToIR(left),
          predicateToIR(right)
        )
      case SQL.OR(left, right) =>
        SQLIR.OR(
          predicateToIR(left),
          predicateToIR(right)
        )
      case SQL.EQ(left, right) =>
        SQLIR.EQ(
          expressionToIR(left),
          expressionToIR(right)
        )
      case SQL.LT(left, right) =>
        SQLIR.LT(
          expressionToIR(left),
          expressionToIR(right)
        )
      case SQL.LE(left, right) =>
        SQLIR.LE(
          expressionToIR(left),
          expressionToIR(right)
        )

  private def expressionToIR(using ctx: IRContext)(
      e: SQL.Expression
  ): SQLIR.Expression =
    e match
      case SQL.ColumnRef(t, c) =>
        SQLIR.ProjectionExpression(ctx.fromColumn(ColumnRef(t, c))) // TODO: This should not allow a star to pass through
      case SQL.Function(SQL.FunctionLabel(symbol), args) =>
        SQLIR.Function(
          SQLIR.FunctionLabel(symbol),
          args.map(expressionToIR(_))
        )
      case SQL.Aggregate(SQL.AggregateLabel(symbol), arg) =>
        SQLIR.Aggregate(
          SQLIR.AggregateLabel(symbol),
          queryToIR(arg)
        )
      case SQL.Constant(value) => SQLIR.Constant(value)
    
end SQLToIR

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
  val ctx = ???

  @main def testDenotation =
    println(testQuery)
    println(SQLToIR.translate(using ctx)(testQuery))
    // println(DenotationIRToUSR.irQueryToUSR(DenotationSQLToIR.sqlToIR(testQuery, testSchema)))
    // println(testQuery2)
    // println(DenotationSQLToIR.sqlToIR(testQuery2, testSchema))
    // println(DenotationIRToUSR.irQueryToUSR(DenotationSQLToIR.sqlToIR(testQuery2, testSchema)))
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

    val innerQueries = s.from.clauses
    val variableFromLabel = (l: QueryLabel) => USR.Variable(l.name)

    // FROM and nested query constraints
    val innerConstraints = innerQueries.map((q, l) => irQueryToUSR(q).substituted((queryVar, variableFromLabel(l))))
    val innerVariables = innerQueries.map((_, l) => variableFromLabel(l))

    // SELECT constraints
    // collect the columns to be selected, and turn them into a tree
    val toSelect = 
      s.selection match
        case Star => innerVariables
        case CompoundProjection(ps) => ps.map(irProjectionToValue(_))
        case _ => Seq(irProjectionToValue(s.selection))
    
    /**
      * Takes a list of USR Values to select, and interprets them as a tree
      * constraint on the query variable
      *
      * c1 c2 c3 => (t.left = c1) * (t.right.left = c2) * (t.right.right = c3)
      */
    def toConstraints(s: List[USR.Value], currentNode: USR.Value = queryVar): List[USR.Expression] =
      s match
        case Nil => USR.One :: Nil
        case head :: Nil => USR.EQ(currentNode, head) :: Nil
        case head :: neck :: Nil => USR.EQ(USR.Left(currentNode), head) :: USR.EQ(USR.Right(currentNode), head) :: Nil
        case head :: next => USR.EQ(USR.Left(currentNode), head) :: toConstraints(next, USR.Right(currentNode))
      
    val selectionConstraints = toConstraints(toSelect.toList).toSeq
      
    val summationExpression = USR.Mul(selectionConstraints ++ innerConstraints :+ addedConstraints)

    // add required summations outside
    innerVariables.foldLeft(summationExpression: USR.Expression) ((e, v) => USR.USum(v, e))

}
