package ckite.util

import java.util.concurrent.{ Executors, ScheduledExecutorService }

trait ConcurrencySupport {

  def scheduler(name: String): ScheduledExecutorService = {
    Executors.newScheduledThreadPool(1, CustomThreadFactory(name))
  }

}
