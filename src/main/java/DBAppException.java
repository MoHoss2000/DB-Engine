public class DBAppException extends Exception{
    private static final long serialVersionUID = 1L;

    public DBAppException(){
        super();
    }

    public DBAppException(String message){
        super(message);
    }
}
