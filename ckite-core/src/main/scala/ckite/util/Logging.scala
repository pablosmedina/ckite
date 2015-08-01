package ckite.util

import org.slf4j.LoggerFactory

trait Logging {

  val logger = LoggerFactory.getLogger(this.getClass())

  def loggingErrors[T](f: ⇒ T) = {
    try {
      f
    } catch {
      case e: Exception ⇒ logger.error("Error", e); throw e
    }
  }

}