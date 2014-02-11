package ckite.util

import java.util.concurrent.Callable

object CKiteConversions {
  
	implicit def fromFunctionToRunnable(f: () => Any) : Runnable = new Runnable() { override def run() = {  f() }}
	implicit def fromFunctionToCallable[V](f: () => V) : Callable[V] = new Callable[V]() { override def call() = {  f() }}
	
}
