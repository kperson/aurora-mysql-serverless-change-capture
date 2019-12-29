package com.github.kperson.cc

import java.sql.DriverManager

object Main extends App {

  val connection = DriverManager
    .getConnection("jdbc:mysql://localhost/change_capture?user=root&password=123456")

  val service: SchemaService = new MySQLSchemaService(connection, "change_capture")

  val schema = service.schema.withExclusion(Exclusion(List(), Map("my_user" -> List("id"))))

  val schemaWithoutMetaTables = schema.copy(tables = schema.tables.filterNot { t =>
    t.name.toLowerCase == "cc_log" || t.name.toLowerCase == "cc_log_meta"
  })

  println(schemaWithoutMetaTables)
  val ddlSQL = TriggerSchemaScript(schemaWithoutMetaTables)
  connection.setAutoCommit(false)

  ddlSQL.foreach { sql =>
    val st = connection.createStatement()
    st.execute(sql)
  }

  connection.commit()
  //connection.close()

}