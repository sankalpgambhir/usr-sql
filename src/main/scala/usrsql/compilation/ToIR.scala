package usrsql.compilation

object ToIR:
    import usrsql.language.* 
    import usrsql.language.SQLIR.*
    import usrsql.language.USR.*

    object UnreachablePatternException extends Exception

    // Issues:
    // do you see a + ? is it a UNIONALL or an OR? <== alternating search modes?
    // recovering select columns <== ok maybe

    def translate(expr: Formula): Query = 
        expr match
            case App(Lambda(t, formula, columns), _) =>
                encodeQuery(formula, columns, t, Seq.empty)
            case _ => throw UnreachablePatternException

    def mapFormulaToQuery(expr: Formula): Query =
        // in query search mode, a + is a UNION
        expr match
            case App(Lambda(t1, Add(innerQueries), _), t2) =>
                // union
                // t1 == t2 by construction
                val left = innerQueries(0)
                val right = innerQueries(1)
                UNIONALL(mapFormulaToQuery(left), mapFormulaToQuery(right))
            case App(Lambda(t1, formula, columns), _) => 
                // select statement
                encodeQuery(formula, columns, t1, Seq.empty)
            case _ => throw IllegalArgumentException("A top level formula must be a lambda to form a query.")

    /**
      * Encodes a formula into a query
      *
      * @param expr expression to encode
      * @param columns columns to select
      * @param queryVar top-level variable to assume as query output
      * @param accVars accumulated bounded variables from summations
      */
    def encodeQuery(expr: Formula, columns: Seq[Value], queryVar: TermVariable, accVars: Seq[TermVariable]): Query =
        expr match
            case Add(innerQueries) =>
                // Union
                // columns == Seq.empty
                val left = innerQueries(0)
                val right = innerQueries(1)
                // extract columns and repeat
                UNIONALL(translate(left), translate(right))
            case Mul(Seq(left, Not(Seq(right)))) =>
                EXCEPT(translate(left), translate(right))
            case Squash(inner) =>
                encodeQuery(inner.head, columns, queryVar, accVars) match
                    case SELECT(
                        columns,
                        from,
                        where,
                        distinct
                    ) => SELECT(columns, from, where, true)
                    case _ => throw UnreachablePatternException
            case Sum(t, inner) => 
                encodeQuery(inner, columns, queryVar, accVars :+ t)
            case inner =>
                // we have resolved summations and top level unions etc
                // this is a select statement
                // FROM: for each summation variable, find a table or inner query
                // we don't actually use accVars as we assume a valid constraint exists
                // WHERE: collect all remaining predicates
                val (fromRaw, whereRaw) = destructExpression(inner, queryVar)
                val from = fromRaw.map {
                    case a: App =>
                        QueryAlias(translate(a), a.to.label.name)
                    case Relation(tname, TermVariable(VariableTermLabel(name))) =>
                        TableAlias(Table(tname), name)
                    case _ => throw UnreachablePatternException
                }
                val where = convertExpr(whereRaw.get)
                val distinct = false
                SELECT(
                    columns,
                    from,
                    where,
                    distinct
                )
    def destructExpression(expr: Formula, queryVar: TermVariable): (Seq[Formula], Option[Formula]) = 
        def containsVar(value: Term): Boolean =
            value match
                case TermVariable(VariableTermLabel(name)) => name == queryVar.label.name
                case Function(_, inner) => inner.exists(containsVar(_))
        expr match
            // from clauses
            case a: App => (Seq(a), None)
            case r @ Relation(_, _) => (Seq(r), None)
            // where clauses
            // any clauses involving the query variable can discarded
            case Predicate(_, Seq(value, _)) if containsVar(value) => (Seq.empty, None)
            case Predicate(_, Seq(_, value)) if containsVar(value) => (Seq.empty, None)
            // other base cases
            case p: USR.Predicate => (Seq.empty, Some(p))
            case Zero => (Seq.empty, Some(Zero))
            case One => (Seq.empty, Some(One))
            case Not(Seq(inner)) => 
                val (_, innerDestructed) = destructExpression(inner, queryVar)
                (Seq.empty, Some(Not(innerDestructed.get)))
            case Squash(Seq(inner)) =>
                val (_, innerDestructed) = destructExpression(inner, queryVar)
                (Seq.empty, Some(Squash(innerDestructed.get)))
            case Add(Seq(left, right)) =>
                val (f1, i1) = destructExpression(left, queryVar)
                val (f2, i2) = destructExpression(right, queryVar)
                (f1 ++ f2, Some(Add(i1.get, i2.get)))
            case Mul(Seq(left, right)) =>
                val (f1, i1) = destructExpression(left, queryVar)
                val (f2, i2) = destructExpression(right, queryVar)
                val leftOver = 
                    (i1, i2) match
                        case (None, None) => None
                        case (None, _) => i2
                        case (_, None) => i1
                        case _ => Some(Mul(i1.get, i2.get))
                    
                (f1 ++ f2, leftOver)
            case Sum(t, inner) =>
                val (_, innerDestructed) = destructExpression(inner, queryVar)
                (Seq.empty, Some(Sum(t, innerDestructed.get)))
            case Connector(_, _) => 
                throw IllegalStateException("Arbitrary connectors should not exist.")
            case FormulaVariable(_) => 
                throw IllegalStateException("Formula variables are not in use.")
                
    def convertExpr(expr: Formula): SQLIR.Predicate =
        expr match
            case Zero => FALSE
            case One => TRUE
            case Equality(Seq(left, right)) => EQ(convertValue(left), convertValue(right))
            case Less(Seq(left, right)) => LT(convertValue(left), convertValue(right))
            case LessOrEqual(Seq(left, right)) => LE(convertValue(left), convertValue(right))
            case Add(Seq(left, right)) => OR(convertExpr(left), convertExpr(right)) 
            case Mul(Seq(left, right)) => AND(convertExpr(left), convertExpr(right)) 
            case Not(Seq(inner)) => NOT(convertExpr(inner))
            case Squash(q @ App(_, _)) => EXISTS(translate(q))
            case Sum(_, _) => throw UnreachablePatternException 
            case _ => throw UnreachablePatternException

    def convertValue(t: Term): SQLIR.Value =
        t match
            case TermVariable(VariableTermLabel(name)) => TableProjection(Table(name))
            case Function(USR.Left, Seq(inner)) => SQLIR.Left(convertValue(inner).asInstanceOf[Projection])
            case Function(USR.Right, Seq(inner)) => SQLIR.Right(convertValue(inner).asInstanceOf[Projection])
            case USR.Constant(data) => SQLIR.Constant(data)

end ToIR
