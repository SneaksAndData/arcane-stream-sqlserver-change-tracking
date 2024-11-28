package com.sneaksanddata.arcane.framework
package exceptions

/**
 * Represents an exception that occurs when there is an issue with the configuration of the application.
 */
class ConfigurationException extends Exception:
  
  /**
   * Constructs a new ConfigurationException with the specified message and cause.
   *
   * @param message The message of the exception.
   * @param cause The cause of the exception.
   */
  def this(message: String, cause: Throwable) =
    this()
    initCause(cause)

  /**
   * Constructs a new ConfigurationException with the specified message.
   * @param message
   */
  def this(message: String) =
    this()
    initCause(null)
