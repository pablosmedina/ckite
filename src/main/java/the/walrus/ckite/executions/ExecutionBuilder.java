package the.walrus.ckite.executions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecutionBuilder {

	private Collection<Callable<?>> tasks = new ArrayList<Callable<?>>();
	private ExecutorService executor;
	private List<Timeout> timeouts = new ArrayList<Timeout>();
	private Integer expectedResults; 
	
	public ExecutionBuilder withTask(Callable<?> task) {
		tasks.add(task);
		return this;
	}

	public ExecutionBuilder withTimeout(long timeout, TimeUnit timeUnit) {
		timeouts.add(new Timeout(timeout, timeUnit));
		return this;
	}
	
	/**
	 * Executes the given tasks and joins to the results.
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public <T>Collection<T> execute() throws InterruptedException, ExecutionException {
		final CountDownLatch countDownLatch = createCountDownLatch();
		Collection<Future<T>> results = submitCallables(countDownLatch);
		await(countDownLatch);
		return collect(results);
	}

	private CountDownLatch createCountDownLatch() {
		int countDown = expectedResults != null ? expectedResults : tasks.size();
		return new CountDownLatch(countDown);
	}

	private void await(final CountDownLatch countDownLatch) throws InterruptedException {
		Timeout timeout = timeouts.iterator().next();
		countDownLatch.await(timeout.getTimeout(), timeout.getTimeUnit());
	}

	private <T> Collection<Future<T>> submitCallables(final CountDownLatch countDownLatch) {
		Collection<Future<T>> results = new ArrayList<Future<T>>();
		ExecutorService executor = obtainExecutor();
		for(final Callable<?> task : tasks) {
			results.add(executor.submit(new Callable<T>() {
				@SuppressWarnings("unchecked")
				@Override
				public T call() throws Exception {
					try { 
						return (T) task.call();
					} finally {
						countDownLatch.countDown();
					}
				}
				
			}));
		}
		return results;
	}
	
	private ExecutorService obtainExecutor() {
		return executor != null ? executor : Executors.newFixedThreadPool(tasks.size());
	}

	public <T>Collection<T> collect(Collection<Future<T>> results) throws InterruptedException, ExecutionException {
		Collection<T> collectedResults = new ArrayList<T>();
		for(Future<T> future : results) {
			 if (future.isDone()) {
				 collectedResults.add(future.get());
			 }
		}
		return collectedResults;
	}

	public ExecutionBuilder withExecutor(ExecutorService executor) {
		this.executor = executor;
		return this;
	}

	public ExecutionBuilder withExpectedResults(int expectedResults) {
		this.expectedResults = expectedResults;
		return this;
	}

}
