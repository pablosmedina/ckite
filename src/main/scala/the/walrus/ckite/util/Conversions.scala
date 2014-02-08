package the.walrus.ckite.util

import java.util.concurrent.Callable

object CKiteConversions {
  
	implicit def fromFunctionToRunnable(f: () => Any) : Runnable = new Runnable() { override def run() = {  f() }}
//	
//	implicit def fromFunctionToCallable[V](f: () => V) : Callable[V] = new Callable[V]() { override def call() = {  f() }}
	
	implicit def fromBlockToCallable[V](f: => V) : Callable[V] = new Callable[V]() { override def call():V = {  f }}
	
	implicit def fromBlockToRunnable[V](f: => V) : Runnable = new Runnable() { override def run() = {  f }}
	
}
