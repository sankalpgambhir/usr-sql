package usrsql.denotation

import usrsql.language.*
import scala.collection.MapView

object ToIR:

  type TableName = String
  type ColumnName = String

  class SQLContext(val schema: Map[TableName, Seq[ColumnName]])

  class TranslationContext(val sqlCtx: SQLContext):

    val columnMap: MapView[TableName, Map[ColumnName, Int]] = sqlCtx.schema.mapValues(_.zipWithIndex.toMap)

    def generateProjection(table: SQL.Table, column: SQL.Column): SQLIR.Projection =
      val tname = table.name
      val cname = column.name
      val columnIndex = columnMap(tname)(cname)
      val maxCount = columnMap(tname).size - 1

      TranslationContext.generateProjection_(SQLIR.TableProjection(SQLIR.Table(tname)), columnIndex, maxCount)

  object TranslationContext:
    @annotation.tailrec
    def generateProjection_(base: SQLIR.Projection, index: Int, max: Int): SQLIR.Projection = 
      if max == 0 then
        // index == 0 too
        base
      else if index == max then
        generateProjection_(SQLIR.Right(base), index - 1, index - 1)
      else
        // index < total
        // index is the left sibling of a much larger subtree
        // trim up to the index
        generateProjection_(SQLIR.Left(base), index, index)

  end TranslationContext

  def translate(using ctx: TranslationContext)(query: SQL.Query): SQLIR.Query =
    mapQuery(query)

  def mapQuery(using ctx: TranslationContext)(query: SQL.Query): SQLIR.Query = 
    query match
      case s: SQL.SELECT => 
        SQLIR.SELECT(
          columns = s.columns.map(mapValue(_)),
          from = s.from.map(mapRef(_)),
          where = mapPredicate(s.where),
          distinct = s.distinct
        )
      case u: SQL.UNIONALL =>
        SQLIR.UNIONALL(
          translate(u.left),
          translate(u.right)
        )
      case e: SQL.EXCEPT =>
        SQLIR.EXCEPT(
          translate(e.left),
          translate(e.right)
        )
    
  def mapValue(using ctx: TranslationContext)(v: SQL.Value): SQLIR.Value = 
    v match
      case SQL.Constant(data) => SQLIR.Constant(data)
      case SQL.ColumnRef(table, column) => ctx.generateProjection(table, column)
    
  def mapRef(using ctx: TranslationContext)(t: SQL.TableRef): SQLIR.TableRef = 
    t match
      case SQL.TableAlias(SQL.Table(tname), name) => SQLIR.TableAlias(SQLIR.Table(tname), name)
      case SQL.QueryAlias(source, name) => SQLIR.QueryAlias(mapQuery(source), name)
    
  def mapPredicate(using ctx: TranslationContext)(p: SQL.Predicate): SQLIR.Predicate = 
    p match
      case SQL.TRUE => SQLIR.TRUE
      case SQL.FALSE => SQLIR.FALSE
      case SQL.EQ(left, right) => SQLIR.EQ(mapValue(left), mapValue(right)) 
      case SQL.LT(left, right) => SQLIR.LT(mapValue(left), mapValue(right)) 
      case SQL.LE(left, right) => SQLIR.LE(mapValue(left), mapValue(right)) 
      case SQL.NOT(inner) => SQLIR.NOT(mapPredicate(inner)) 
      case SQL.EXISTS(inner) => SQLIR.EXISTS(mapQuery(inner)) 
      case SQL.AND(left, right) => SQLIR.AND(mapPredicate(left), mapPredicate(right))
      case SQL.OR(left, right) => SQLIR.OR(mapPredicate(left), mapPredicate(right))

end ToIR
