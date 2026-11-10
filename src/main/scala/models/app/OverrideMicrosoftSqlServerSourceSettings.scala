package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.settings.mssql.DefaultMsSqlServerDatabaseSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultOverrideDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultOverrideSourceBufferingSettings,
  DefaultSourceBufferingSettings,
  OverrideSourceBufferingSettings,
  OverrideStreamSourceSettings
}
import upickle.ReadWriter

/** A partial override of `MicrosoftSqlServerSourceSettings` provided by the Arcane operator via the environment
  * variable.
  */
case class OverrideMicrosoftSqlServerSourceSettings(
    override val buffering: Option[DefaultOverrideSourceBufferingSettings] = None,
    override val configuration: Option[DefaultMsSqlServerDatabaseSourceSettings] = None,
    override val modifications: Option[DefaultOverrideDataRowModificationSettings] = None
) extends OverrideStreamSourceSettings derives ReadWriter:
  override type SourceSettingsOverrideType = DefaultMsSqlServerDatabaseSourceSettings
