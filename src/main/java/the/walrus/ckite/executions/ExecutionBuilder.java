package the.walrus.ckite.executions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ExecutionBuilder {

	private Collection<Callable<?>> tasks = new ArrayList<Callable<?>>();
	private ExecutorService executor;
	private List<Timeout> timeouts = new ArrayList<Timeout>();
	private Integer expectedResults;
	private ExpectedResultFilter expectedResultFilter;
	private CountDownLatch countDownLatch; 
	
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
		countDownLatch = createCountDownLatch();
		BlockingQueue<Future<T>> queue = completionQueue(countDownLatch);
		ExecutorCompletionService<T> completionExecutor = new ExecutorCompletionService<T>(obtainExecutor(), queue);
		Collection<Future<T>> results = submitCallables(completionExecutor);
		await(countDownLatch);
		return collect(results, completionExecutor);
	}
	
	public void stop() {
		countDownLatch.countDown();
	}

	@SuppressWarnings("serial")
	private <T> BlockingQueue<Future<T>> completionQueue(final CountDownLatch countDownLatch) {
		final ExpectedResultFilter expectedResultFilter = this.expectedResultFilter;
		BlockingQueue<Future<T>> queue = new LinkedBlockingQueue<Future<T>>() {
			@Override
			public boolean add(Future<T> e) {
				boolean addResult = super.add(e);
				T result = null;
				try { 
					result = (T) e.get();
				} catch (Exception ex) {
					ex.printStackTrace();
				} finally {
					if (expectedResultFilter == null) {
						countDownLatch.countDown();
					} else {
						int matches = expectedResultFilter.matches(result);
						for(int i = 1; i <= matches; i ++) {
							countDownLatch.countDown();
						}
					}
				}
				return addResult;
			}
		};
		return queue;
	}

	private CountDownLatch createCountDownLatch() {
		int countDown = expectedResults != null ? expectedResults : tasks.size();
		return new CountDownLatch(countDown);
	}

	private boolean await(final CountDownLatch countDownLatch) throws InterruptedException {
		Timeout timeout = timeouts.iterator().next();
		return countDownLatch.await(timeout.getTimeout(), timeout.getTimeUnit());
	}

	private <T> Collection<Future<T>> submitCallables(ExecutorCompletionService<T> completionExecutor) {
		Collection<Future<T>> results = new ArrayList<Future<T>>();
		
		for(final Callable<?> task : tasks) {
			Future<T> future = completionExecutor.submit(new Callable<T>() {
				@SuppressWarnings("unchecked")
				@Override
				public T call() throws Exception {
						return (T) task.call();
				}
			});
			results.add(future);
		}
		return results;
	}
	
	private ExecutorService obtainExecutor() {
		return executor != null ? executor : Executors.newFixedThreadPool(tasks.size());
	}

	public <T>Collection<T> collect(Collection<Future<T>> results, ExecutorCompletionService<T> completionExecutor) throws InterruptedException, ExecutionException {
		Collection<T> collectedResults = new ArrayList<T>();
		Future<T> future = null;
		
		while((future = completionExecutor.poll()) != null) {
			collectedResults.add(future.get());
		}
		return collectedResults;
	}

	public ExecutionBuilder withExecutor(ExecutorService executor) {
		this.executor = executor;
		return this;
	}

	public ExecutionBuilder withExpectedResults(int expectedResults) {
		return withExpectedResults(expectedResults, null);
	}
	
	public ExecutionBuilder withExpectedResults(int expectedResults, ExpectedResultFilter expectedResultFilter) {
		this.expectedResults = expectedResults;
		this.expectedResultFilter = expectedResultFilter;
		return this;
	}

}
