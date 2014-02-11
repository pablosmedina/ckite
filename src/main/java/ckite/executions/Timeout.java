package ckite.executions;

import java.util.concurrent.TimeUnit;

public class Timeout {

	private final long timeout;
	private final TimeUnit timeUnit;

	public Timeout(long timeout, TimeUnit timeUnit) {
		this.timeout = timeout;
		this.timeUnit = timeUnit;
	}

	public long getTimeout() {
		return timeout;
	}
	
	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

}
