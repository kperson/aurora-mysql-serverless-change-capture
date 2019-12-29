package com.github.kperson.cc

import java.sql.Types._

case class Trigger(event: String, objs: List[String])

object TriggerTableScript {


  def apply(table: Table, database: String, shouldCreate: Boolean): List[String] = {
    List("INSERT", "UPDATE", "DELETE").flatMap { e =>
      val h = header(table.name, e)
      val b = body(table.name, table.columns, table.primaryKey, e, 4)
      val f = footer
      if(shouldCreate) {
        h ++ b ++ f
      }
      else {
        h ++ f
      }
    }
  }

  private def header(table: String, event: String): List[String] = {
    List(
      s"LOCK TABLE $table WRITE;",
      s"DROP TRIGGER IF EXISTS `CC_${table.toUpperCase()}_ON_$event`"
    )
  }

  private def generateJSONPayload(obj: String, columns: List[Column]): String = {
    //java.sql.Types
    val rows = columns.flatMap { c =>
      (c.dataTypeJava, c.dataTypeNative) match {
        case (TINYINT | SMALLINT | INTEGER | BIGINT, _)  =>
          Some(s"JSON_KEY_VALUE_INTEGER('${c.name}', ${obj}.`${c.name}`)")
        case (CHAR | VARCHAR | LONGVARCHAR, _)  =>
          Some(s"JSON_KEY_VALUE_STRING('${c.name}', ${obj}.`${c.name}`)")
        case (BINARY | VARBINARY | LONGVARBINARY, _)  =>
          Some(s"JSON_KEY_VALUE_BINARY('${c.name}', ${obj}.`${c.name}`)")
        case (_, "DATETIME") | (DATE, _) => Some(s"JSON_KEY_VALUE_DATETIME('${c.name}', ${obj}.`${c.name}`)")
        case (_, "TIMESTAMP") => Some(s"JSON_KEY_VALUE_TIMESTAMP('${c.name}', ${obj}.`${c.name}`)")
        case (DECIMAL, _) => Some(s"JSON_KEY_VALUE_DECIMAL('${c.name}', ${obj}.`${c.name}`)")
        case (DOUBLE, _) => Some(s"JSON_KEY_VALUE_DOUBLE('${c.name}', ${obj}.`${c.name}`)")
        case (TIME, _) => Some(s"JSON_KEY_VALUE_TIME('${c.name}', ${obj}.`${c.name}`)")
        case _ => None
      }
    }
    s"CONCAT('[', CONCAT_WS(', ', ${rows.mkString(", ")}), ']')"
  }

  private def generatePrimaryKey(obj: String, table: String, columnNames: List[String]): String = {
    val data: List[String] = columnNames.map { c => List(s"'$c'", s"${obj}.`${c}`") }.flatten
    val list = List(s"'$table'") ++ data
    s"CONCAT(${list.mkString(", ")})"
  }

  private def body(
    table: String,
    columns: List[Column],
    primaryKey: List[String],
    event: String,
    numPartitions: Int
  ): List[String] = {
    val newPayload = event match {
      case "UPDATE" | "INSERT" => generateJSONPayload("NEW", columns)
      case _ => "'null'"
    }
    val oldPayload = event match {
      case "UPDATE" | "DELETE" =>  generateJSONPayload("OLD", columns)
      case _ => "'null'"
    }

    val primaryKeyObj = event match {
      case "DELETE" =>  "OLD"
      case _ => "NEW"
    }
    val trigger =
      s"""
        |CREATE TRIGGER `CC_${table.toUpperCase()}_ON_$event` AFTER $event ON `${table}` FOR EACH ROW
        |BEGIN
        |	DECLARE numJobs INT;
        |	DECLARE partitionKey INT;
        |	DECLARE changeValue TEXT;
        | DECLARE primaryKeyHash VARCHAR(255);
        | SET primaryKeyHash = MD5(${generatePrimaryKey(primaryKeyObj, table, primaryKey)});
        | SET partitionKey = CC_PARTITION_VALUE(primaryKeyHash, $numPartitions);
        |	SET changeValue = JSON_CHANGE_CAPTURE(
        |		'${event}',
        |		'$table',
        |		$newPayload,
        |		$oldPayload
        |	);
        |	INSERT INTO cc_log (partition_key, change_value) VALUES (partitionKey, changeValue);
        |	INSERT INTO cc_log_meta (partition_key, num_jobs, locked_at, is_running) VALUES (partitionKey, 1, UNIX_TIMESTAMP(), 0) ON DUPLICATE KEY UPDATE num_jobs = num_jobs + 1;
        |	SET numJobs = (SELECT COUNT(*) FROM cc_log_meta WHERE partition_key = partitionKey AND (locked_at + 60 * 5 < UNIX_TIMESTAMP() OR num_jobs = 1));
        |	IF numJobs = 1 THEN
        |		UPDATE cc_log_meta SET is_running = 1 WHERE partition_key = partitionKey;
        |	END IF;
        |END
        |""".stripMargin

        List(trigger)
  }

  val footer = List("UNLOCK TABLES;")


}

object TriggerSchemaScript {

  def apply(schema: Schema): List[String] = {
    schema.tables.flatMap { t =>
      TriggerTableScript(t, schema.database, true)
    }
  }

}
