package ckite.util

import org.slf4j.LoggerFactory

trait Logging {

  val log = LoggerFactory.getLogger(this.getClass())

  def loggingErrors[T](f: ⇒ T) = {
    try {
      f
    } catch {
      case e: Exception ⇒ log.error("Error", e); throw e
    }
  }

}