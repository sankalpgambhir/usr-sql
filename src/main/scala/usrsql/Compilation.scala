package usrsql

object DenotationUSRToIR {
    import usrsql.USR.*
    import usrsql.SQLIR

    class Context {
        ???
    }

    def usrToSQLIR(using c: Context)(e: Expression): SQLIR.Query =
        val normalized = e.toSumPrefix
        e match
            case Zero => ??? // SELECT * FROM T WHERE FALSE?
            case One => ??? // SELECT DISTINCT * FROM T WHERE TRUE?
            // this will be wrong if T is empty though ^
            case Not(inner) => ???
            // SQLIR.NOT(SQLIR.EXISTS(irToSQLIR(inner)))
            // same issue here?
            case Add(es) => 
                // e1 UNIONALL ...
                assert(!es.isEmpty)
                es.tail.foldLeft(usrToSQLIRInner(es.head)) {
                    case (q, exp) =>
                        SQLIR.UNIONALL(q, usrToSQLIRInner(exp))
                }
            case Mul(es) => ???
            case USum(_, _) => usrToSQLIRInner(e)
            case Squash(inner) => ???
            case RelationRef(r, v) => ???
            case EQ(left, right) => ???
            case LT(left, right) => ???
            case LE(left, right) => ???

    def usrToSQLIRInner(using c: Context)(e: Expression): SQLIR.Query = e match {
        case USum(v, e) => ???
        case _ => throw ???
    }
        
}

object DenotationIRToSQL {
    import usrsql.SQLIR.*
    import usrsql.SQL

    class Context {
        ???

        def getColumnFromIndex(i: Int): SQL.Column = ???
    }

    def irToSQL(using c: Context)(q: SQLIR.Query): SQL.Query =
        q match
            case SELECT(selection, from) => 
                SQL.SELECT(
                    irProjectionToSQL(selection),
                    irFromToSQL(from)
                )
            case FROM(clauses) => throw ??? // shouldn't see a top level FROM
            case WHERE(query, condition) =>
                SQL.WHERE(irToSQL(query), irPredicateToSQL(condition))
            case EXCEPT(left, right) =>
                SQL.EXCEPT(irToSQL(left), irToSQL(right))
            case UNIONALL(left, right) =>
                SQL.UNIONALL(irToSQL(left), irToSQL(right))
            case DISTINCT(query) =>
                SQL.DISTINCT(irToSQL(query))
            case TableRef(table) =>
                SQL.TableRef(SQL.Table(table.name))

    
    def unfoldTreeProjection(using c: Context)(t: Projection, counter: Int = 1): SQL.ColumnProjection =
        t match
            case TableProjection(t) => 
                val name = t.name
                val column = c.getColumnFromIndex(counter)
                SQL.ColumnProjection(SQL.Table(name), column)
            case LeftProjection(of) => unfoldTreeProjection(of, counter * 2) // counter << 1 // TODO: this limits us to n < 32 columns
            case RightProjection(of) => unfoldTreeProjection(of, (counter * 2) + 1) // counter << 1 + 1
            case _ => throw ???

    def irProjectionToSQL(using c: Context)(p: Projection): SQL.Projection = 

        p match
            case Star => SQL.Star
            case TableProjection(t) => SQL.ColumnProjection(SQL.Table(t.name), SQL.Star) // select entire table
            case LeftProjection(_) => unfoldTreeProjection(p)
            case RightProjection(_) => unfoldTreeProjection(p)
            case CompoundProjection(ps) => SQL.CompoundProjection(ps.map(irProjectionToSQL(_)))
            case ExpressionProjection(exp) => SQL.LabelledProjection(irExpressionToSQL(exp), SQL.ProjectionLabel("TODO"))
        
    def irFromToSQL(using c: Context)(f: FROM): SQL.FROM = 
        val clauses = f.clauses
        val newClauses = clauses.map {
            case (q, l) => (irToSQL(q), SQL.QueryLabel(l.name))
        }
        SQL.FROM(newClauses)

    def irExpressionToSQL(using c: Context)(e: Expression): SQL.Expression = 
        e match
            case ProjectionExpression(p) => 
                val columnProj = unfoldTreeProjection(p)
                columnProj match {
                    case SQL.ColumnProjection(t, c: SQL.Column) => SQL.ColumnRef(t, c)
                    case _ => throw ??? // shouldn't see a star 
                }
                
            case Function(label, args) => SQL.Function(SQL.FunctionLabel(label.name), args.map(irExpressionToSQL(_)))
            case Aggregate(label, arg) => SQL.Aggregate(SQL.AggregateLabel(label.name), irToSQL(arg))
            case Constant(value) => SQL.Constant(value)
        
    def irPredicateToSQL(using c: Context)(e: Predicate): SQL.Predicate = 
        e match
            case TRUE => SQL.TRUE
            case FALSE => SQL.FALSE
            case NOT(inner) => SQL.NOT(irPredicateToSQL(inner))
            case EXISTS(inner) => SQL.EXISTS(irToSQL(inner))
            case AND(left, right) =>
                SQL.AND(
                irPredicateToSQL(left),
                irPredicateToSQL(right)
                )
            case OR(left, right) =>
                SQL.OR(
                irPredicateToSQL(left),
                irPredicateToSQL(right)
                )
            case EQ(left, right) =>
                SQL.EQ(
                irExpressionToSQL(left),
                irExpressionToSQL(right)
                )
            case LT(left, right) =>
                SQL.LT(
                irExpressionToSQL(left),
                irExpressionToSQL(right)
                )
            case LE(left, right) =>
                SQL.LE(
                irExpressionToSQL(left),
                irExpressionToSQL(right)
                )
        
        
}
