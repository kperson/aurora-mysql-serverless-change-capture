package com.github.kperson.cc

import java.sql.ResultSet

object ResultSetExtStream {

  implicit class ResultSetExStreamDef(self: ResultSet) {

    def stream: Stream[ResultSet] = {
      Stream.continually(if(self.next()) Some(self) else None).takeWhile(_.isDefined).collect {
        case Some(s) => s
      }
    }

  }

}
