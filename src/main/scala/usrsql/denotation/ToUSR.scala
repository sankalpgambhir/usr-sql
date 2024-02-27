package usrsql.denotation

object ToUSR:
  import usrsql.language.*
  import usrsql.language.SQLIR.*
  import usrsql.language.USR.*

  var lambdaCount = 0

  val topVariable = TermVariable(VariableTermLabel("t"))
  def dbVar(depth: Int) = 
    lambdaCount += 1
    TermVariable(VariableTermLabel(s"lambda$lambdaCount"))
  def tableVar(name: String) = TermVariable(VariableTermLabel(name))

  def translate(query: Query): Formula = 
    App(mapQuery(query, 0), topVariable)

  def mapQuery(query: Query, depth: Int): Lambda = 
    query match
      case EXCEPT(left, right) =>
        val absVar = dbVar(depth)
        Lambda(absVar, App(mapQuery(left, depth + 1), absVar) * !App(mapQuery(right, depth + 1), absVar), Seq.empty)
      case UNIONALL(left, right) =>
        val absVar = dbVar(depth)
        Lambda(absVar, App(mapQuery(left, depth + 1), absVar) + App(mapQuery(right, depth + 1), absVar), Seq.empty)
      case SELECT(
        columns,
        from,
        where,
        distinct
      ) =>
        val absVar = dbVar(depth)
        val summationVariables = from.map(ref => TermVariable(VariableTermLabel(ref.name)))
        val sourceConstraints = from.map(mapRef(_, depth + 1))
        val columnConstraints = {
          val numCols = columns.length
          val baseProjection = SQLIR.TableProjection(Table(absVar.label.name))
          columns.zipWithIndex.map { case (c, i) => 
            val queryProj = ToIR.TranslationContext.generateProjection_(baseProjection, i, numCols)
            Equality(mapValue(queryProj), mapValue(c))
          }
        }
        val predicateConstraints = mapPredicate(where, depth + 1)
        val inner = (sourceConstraints ++ columnConstraints).foldLeft(predicateConstraints)(_ * _)
        val base = summationVariables.foldLeft(inner)((f, v) => Sum(v, f))
        val body = if distinct then Squash(base) else base
        Lambda(absVar, body, columns)

  def mapRef(ref: TableRef, depth: Int): Formula = 
    ref match
      case TableAlias(Table(tname), name) => 
        val variable = TermVariable(VariableTermLabel(name))
        Relation(tname)(variable)
      case QueryAlias(query, name) => 
        val variable = TermVariable(VariableTermLabel(name))
        App(mapQuery(query, depth), variable)
    
  def mapPredicate(ref: SQLIR.Predicate, depth: Int): Formula = 
    ref match
      case TRUE => One
      case FALSE => Zero
      case NOT(inner) => !mapPredicate(inner, depth)
      case EXISTS(query) => 
        val lambda = mapQuery(query, depth)
        Squash(Sum(lambda.variable, lambda.body))
      case AND(left, right) =>
        mapPredicate(left, depth) * mapPredicate(right, depth)
      case OR(left, right) =>
        mapPredicate(left, depth) + mapPredicate(right, depth)
      case EQ(left, right) =>
        Equality(mapValue(left), mapValue(right))
      case LT(left, right) =>
        Less(mapValue(left), mapValue(right))
      case LE(left, right) =>
        LessOrEqual(mapValue(left), mapValue(right))

    
  def mapValue(v: SQLIR.Value): Term = 
    v match
      case SQLIR.Constant(data) => USR.Constant(data)
      case p: SQLIR.Projection => mapProjection(p)
    
  def mapProjection(proj: SQLIR.Projection): Term = 
    proj match
      case SQLIR.TableProjection(SQLIR.Table(tname)) => tableVar(tname)
      case SQLIR.Left(inner) => USR.Left(mapProjection(inner))
      case SQLIR.Right(inner) => USR.Right(mapProjection(inner))
    
end ToUSR

