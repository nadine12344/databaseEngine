package T_221B;

public class DBAppException extends Exception {

	public DBAppException(String string) {
		System.out.println(string);
	}
	public DBAppException() {
		super();
	}

}
