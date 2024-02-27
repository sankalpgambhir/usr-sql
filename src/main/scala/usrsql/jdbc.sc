//> using dep org.duckdb:duckdb_jdbc:0.8.1

import java.sql.DriverManager
import java.sql.Connection

object ScalaJdbcConnectSelect {

  def main(args: Array[String]): Unit = {
    // connect to the database named "mysql" on the localhost
    val driver = "org.duckdb.DuckDBDriver"
    val url = "jdbc:duckdb:"

    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
  }

}

ScalaJdbcConnectSelect.main(Array("a"))
