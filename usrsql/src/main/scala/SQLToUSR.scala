package usrsql

import usrsql.USRLanguage.*
import usrsql.SQLLanguage.*

object SQLToUSR {
  def toUSR(q: SQLExpression): USRExpression =
    q match
      case Select(selectType, selectExpression, from, where, groupBy) => ???

}
