package the.walrus.ckite.executions;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import the.walrus.ckite.executions.Executions;

public class ExecutionsTest {

	@Test
	public void testTasksExecutedInTime() throws Exception {
		Collection<String> results = 
		Executions.newExecution().withTask(aTaskLastingSeconds(1))
								 .withTask(aTaskLastingSeconds(2))
								 .withTimeout(5,TimeUnit.SECONDS)
								 .withExecutor(Executors.newFixedThreadPool(2)).execute();
		
		assertEquals(2, results.size());
		assertTrue(results.contains("Task-1s"));
		assertTrue(results.contains("Task-2s"));
	}
	
	
	@Test
	public void testTaskTimedOut() throws Exception {
		Collection<String> results = 
		Executions.newExecution().withTask(aTaskLastingSeconds(1))
								 .withTask(aTaskLastingSeconds(15))
								 .withTimeout(2,TimeUnit.SECONDS)
								 .withExecutor(Executors.newFixedThreadPool(2)).execute();
		
		assertEquals(1, results.size());
		assertEquals("Task-1s", results.iterator().next());
	}
	
	@Test
	public void testAwaitTimedOutWihoutResults() throws Exception {
		Collection<String> results = 
		Executions.newExecution().withTask(aTaskLastingSeconds(3))
								 .withTask(aTaskLastingSeconds(4))
								 .withTimeout(1,TimeUnit.SECONDS)
								 .withExecutor(Executors.newFixedThreadPool(2)).execute();
		
		assertTrue(results.isEmpty());
	}
	
	private Callable<String> aTaskLastingSeconds(final int seconds) {
		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				Thread.sleep(seconds*1000);
				return "Task-"+seconds+"s";
			}
		
		};
	}
	
}
