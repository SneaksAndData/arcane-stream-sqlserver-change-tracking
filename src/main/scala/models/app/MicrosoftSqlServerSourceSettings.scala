package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.settings.DefaultFieldSelectionRuleSettings
import com.sneaksanddata.arcane.framework.models.settings.mssql.DefaultMsSqlServerDatabaseSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import upickle.ReadWriter

case class MicrosoftSqlServerSourceSettings(
                                        override val buffering: DefaultSourceBufferingSettings,
                                        override val fieldSelectionRule: DefaultFieldSelectionRuleSettings,
                                        override val configuration: DefaultMsSqlServerDatabaseSourceSettings
                                      ) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultMsSqlServerDatabaseSourceSettings
