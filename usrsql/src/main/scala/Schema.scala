package usrsql
object Schema {
  // data types
  enum DataType {
    case Int(val value: scala.Int)
    case Bool(val value: scala.Boolean)
    case String(val value: java.lang.String)
  }

  // schema types
  sealed trait SchemaType

  // linear or list like
  case class Linear(tables: Map[String, Seq[(String, DataType)]])

  // tree like
  case class Tree(tables: Map[String, BTree[DataType]])

  // simple binary tree
  sealed trait BTree[A] {
    def value: Option[A] =
      this match
        case Empty       => None
        case Leaf(label) => Some(label)
        case Node(l, r)  => None

    def left: Option[BTree[A]] =
      this match
        case Empty       => None
        case Leaf(label) => None
        case Node(l, r)  => Some(l)

    def right: Option[BTree[A]] =
      this match
        case Empty       => None
        case Leaf(label) => None
        case Node(l, r)  => Some(r)

  }

  case object Empty extends BTree[Nothing]
  case class Leaf[A](val label: A) extends BTree[A]
  case class Node[A](val l: BTree[A], val r: BTree[A]) extends BTree[A]
}
