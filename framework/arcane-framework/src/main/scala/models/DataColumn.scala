package com.sneaksanddata.arcane.framework
package models

/**
 * Represents a row of data.
 */
type DataRow = List[DataCell]

/**
 * Represents a row of data.
 *
 * @param name The name of the row.
 * @param Type The type of the row.
 * @param value The value of the row.
 */
case class DataColumn(name: String, Type: ArcaneType, value: Any)

/**
 * Companion object for [[DataColumn]].
 */
object DataColumn:
  def apply(name: String, Type: ArcaneType, value: Any): DataColumn = new DataColumn(name, Type, value)