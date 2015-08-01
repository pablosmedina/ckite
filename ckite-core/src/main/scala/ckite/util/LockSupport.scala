package ckite.util

import java.util.concurrent.locks.ReentrantReadWriteLock

trait LockSupport {

  private val lock = new ReentrantReadWriteLock()
  private val exclusiveLock = lock.writeLock()
  private val sharedLock = lock.readLock()

  def shared[T](block: ⇒ T): T = {
    sharedLock.lock()
    try {
      block
    } finally {
      sharedLock.unlock()
    }
  }

  def exclusive[T](block: ⇒ T): T = {
    exclusiveLock.lock()
    try {
      block
    } finally {
      exclusiveLock.unlock()
    }
  }

}
