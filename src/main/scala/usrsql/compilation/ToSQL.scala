package usrsql.compilation

import usrsql.language.*

object ToSQL:

  type TableName = String
  type ColumnName = String

  class SQLContext(val schema: Map[TableName, Seq[ColumnName]])

  class TranslationContext(val sqlCtx: SQLContext):
    def unproject(proj: SQLIR.Projection): SQL.ColumnRef = 
      val (baseTable, projectionIndex) = unproject_(proj, 0)
      val table = SQL.Table(baseTable)
      val column = SQL.Column(sqlCtx.schema(baseTable)(projectionIndex))
      SQL.ColumnRef(table, column)

    @annotation.tailrec
    private def unproject_(proj: SQLIR.Projection, acc: Int): (TableName, Int) = 
      proj match
        case SQLIR.TableProjection(SQLIR.Table(tname)) => (tname, acc)
        case SQLIR.Left(inner) => unproject_(inner, acc)
        case SQLIR.Right(inner) => unproject_(inner, acc + 1)

  end TranslationContext

  def translate(using ctx: TranslationContext)(query: SQLIR.Query): SQL.Query = 
    mapQuery(query)

  def mapQuery(using ctx: TranslationContext)(query: SQLIR.Query): SQL.Query = 
    query match
      case s: SQLIR.SELECT => 
        SQL.SELECT(
          columns = s.columns.map(mapValue(_)),
          from = s.from.map(mapRef(_)),
          where = mapPredicate(s.where),
          distinct = s.distinct
        )
      case u: SQLIR.UNIONALL =>
        SQL.UNIONALL(
          translate(u.left),
          translate(u.right)
        )
      case e: SQLIR.EXCEPT =>
        SQL.EXCEPT(
          translate(e.left),
          translate(e.right)
        )
    
  def mapValue(using ctx: TranslationContext)(v: SQLIR.Value): SQL.Value = 
    v match
      case SQLIR.Constant(data) => SQL.Constant(data)
      case p: SQLIR.Projection => ctx.unproject(p)
    
  def mapRef(using ctx: TranslationContext)(t: SQLIR.TableRef): SQL.TableRef = 
    t match
      case SQLIR.TableAlias(SQLIR.Table(tname), name) => SQL.TableAlias(SQL.Table(tname), name)
      case SQLIR.QueryAlias(source, name) => SQL.QueryAlias(mapQuery(source), name)
    
  def mapPredicate(using ctx: TranslationContext)(p: SQLIR.Predicate): SQL.Predicate = 
    p match
      case SQLIR.TRUE => SQL.TRUE
      case SQLIR.FALSE => SQL.FALSE
      case SQLIR.EQ(left, right) => SQL.EQ(mapValue(left), mapValue(right)) 
      case SQLIR.LT(left, right) => SQL.LT(mapValue(left), mapValue(right)) 
      case SQLIR.LE(left, right) => SQL.LE(mapValue(left), mapValue(right)) 
      case SQLIR.NOT(inner) => SQL.NOT(mapPredicate(inner)) 
      case SQLIR.EXISTS(inner) => SQL.EXISTS(mapQuery(inner)) 
      case SQLIR.AND(left, right) => SQL.AND(mapPredicate(left), mapPredicate(right))
      case SQLIR.OR(left, right) => SQL.OR(mapPredicate(left), mapPredicate(right))

end ToSQL