package com.github.kperson.cc

import java.sql.Connection

import ResultSetExtStream._

case class Column(name: String, isNullable: Boolean, dataTypeJava: Short, dataTypeNative: String)
case class Table(name: String, columns: List[Column], primaryKey: List[String])
case class Schema(database: String, tables: List[Table]) {

  def withExclusion(exclusion: Exclusion): Schema = {
    val ts = tables
    .filter { t => !exclusion.excludedTables.contains(t.name) }
    .map { t =>
      val excludedColumns = exclusion.excludeColumns.getOrElse(t.name, List.empty)
      val cols = t.columns.filter { c => !excludedColumns.contains(c) }
      Table(t.name, cols, t.primaryKey)
    }
    Schema(database, ts)
  }

}

trait SchemaService {

  def schema: Schema




}

class MySQLSchemaService(connection: Connection, database: String) extends SchemaService {

  def primaryKey(table: String): List[String] = {
    val query ="""
      |SELECT COLUMN_NAME
      |      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      |      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME='PRIMARY'
      |""".stripMargin

    val statement = connection.prepareStatement(query)
    statement.setString(1, database)
    statement.setString(2, table)

    val rs = statement.executeQuery()
    rs.stream.map { _.getString(1) }.toList.sortWith { (a, b) => a < b }
  }

  def schema: Schema = {
    val meta = connection.getMetaData
    val tableNames = meta.getTables(connection.getCatalog, null, "%", null).stream.map { _.getString(3) }
    val tables = tableNames.map { t =>
      val rs = meta.getColumns(null, null, t, null)
      val cols = rs.stream.map { x =>
        //http://www.herongyang.com/JDBC/sqljdbc-jar-Column-List.html
        Column(x.getString("COLUMN_NAME"), x.getShort("NULLABLE") == 1,  x.getShort("DATA_TYPE"), x.getString(6))
      }.toList
      Table(t, cols, primaryKey(t))
    }.toList
    Schema(connection.getCatalog, tables)
  }

}