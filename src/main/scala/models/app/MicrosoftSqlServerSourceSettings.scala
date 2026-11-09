package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import com.sneaksanddata.arcane.framework.models.settings.mssql.DefaultMsSqlServerDatabaseSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import upickle.ReadWriter

case class MicrosoftSqlServerSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val modifications: DefaultDataRowModificationSettings,
    override val configuration: DefaultMsSqlServerDatabaseSourceSettings
) extends StreamSourceSettings, Mergeable derives ReadWriter:
  override type SourceSettingsType = DefaultMsSqlServerDatabaseSourceSettings
  
  override type MergeableFrom = OverrideMicrosoftSqlServerSourceSettings
  override type MergeResult = MicrosoftSqlServerSourceSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    MicrosoftSqlServerSourceSettings(
      buffering = this.buffering.merge(overrides.flatMap(_.buffering)),
      modifications = this.modifications.merge(overrides.flatMap(_.modifications)),
      configuration
    )

