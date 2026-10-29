package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.settings.mssql.DefaultMsSqlServerDatabaseSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultOverrideDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultSourceBufferingSettings,
  OverrideStreamSourceSettings
}
import upickle.ReadWriter

/** A partial override of `MicrosoftSqlServerSourceSettings` provided by the Arcane operator via the environment
  * variable. Not applied yet, see the TODO note on `MicrosoftSqlServerPluginStreamContext.merge`.
  */
case class OverrideMicrosoftSqlServerSourceSettings(
    override val configuration: Option[DefaultMsSqlServerDatabaseSourceSettings] = None,
    override val buffering: Option[DefaultSourceBufferingSettings] = None,
    override val modifications: Option[DefaultOverrideDataRowModificationSettings] = None
) extends OverrideStreamSourceSettings derives ReadWriter:
  override type SourceSettingsOverrideType = DefaultMsSqlServerDatabaseSourceSettings
