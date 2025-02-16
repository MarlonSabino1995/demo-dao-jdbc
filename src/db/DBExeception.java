package db;

public class DBExeception extends RuntimeException{

	private static final long serialVersionUID = 1L;

	public DBExeception (String msg) {
		super(msg);
	}
}
