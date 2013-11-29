package the.walrus.ckite.executions;

public class ExpectedResultFilter {

	private final Object expected;

	public ExpectedResultFilter(Object expected) {
		this.expected = expected;
	}
	
	public boolean matches(Object o) {
		return o != null && expected.equals(o);
	}
	
}
