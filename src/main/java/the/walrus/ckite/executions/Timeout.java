package the.walrus.ckite.executions;

import java.util.concurrent.TimeUnit;

public class Timeout {

	private final long timeout;
	private final TimeUnit timeUnit;
	private final TimeoutHandler handler;

	public Timeout(long timeout, TimeUnit timeUnit) {
		this(timeout, timeUnit, null);
	}
	
	public Timeout(long timeout, TimeUnit timeUnit, TimeoutHandler handler) {
		this.timeout = timeout;
		this.timeUnit = timeUnit;
		this.handler = handler;
	}

	public long getTimeout() {
		return timeout;
	}
	
	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

}
