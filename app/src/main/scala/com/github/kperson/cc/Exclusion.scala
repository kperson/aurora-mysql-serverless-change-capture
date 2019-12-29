package com.github.kperson.cc

case class Exclusion(
  excludedTables: List[String] = List.empty,
  excludeColumns: Map[String, List[String]] = Map.empty
)
