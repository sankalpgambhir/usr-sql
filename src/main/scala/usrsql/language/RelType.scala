package usrsql.language

object RelType:
  
  sealed trait BaseType
  case object IntType extends BaseType

  sealed trait RelType
  case class Leaf(tpe: BaseType) extends RelType
  case class Node(left: RelType, right: RelType) extends RelType

  trait Typed:
    def tpe: RelType

  case object InvalidTypeException extends Exception

