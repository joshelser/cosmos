package cosmos;

public class UnexpectedStateException extends Exception {

  private static final long serialVersionUID = 1L;

  public UnexpectedStateException() {
    super();
  }
  
  public UnexpectedStateException(String message) {
    super(message);
  }
  
}
