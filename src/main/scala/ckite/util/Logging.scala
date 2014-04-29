package ckite.util

import org.slf4j.LoggerFactory

trait Logging {

  val LOG = LoggerFactory.getLogger(this.getClass())
  
  def loggingErrors[T](f: => T) = {
    try {
      f
    } catch {
      case e: Exception => LOG.error("Error", e); throw e
    }
  }
  
}