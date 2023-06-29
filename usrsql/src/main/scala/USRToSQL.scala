package usrsql

import usrsql.USRLanguage.*
import usrsql.SQLLanguage.*

object USRToSQL {
  def collectRelations(e: USRExpression): Seq[Relation] = ???

  def toSQL(e: USRExpression): Option[SQLExpression] =
    val eUniq = eliminateUnusedVars(e.toSumPrefix).map(_.toSumPrefix)
    if eUniq.isEmpty then throw NonSQLExpressionException

    val exp: USRExpression = eUniq.get

    val tables: Seq[USRExpression] = collectRelations(exp)
    // should contain raw tables as well as nested queries?
    val fromClause = ???

    ???
}
