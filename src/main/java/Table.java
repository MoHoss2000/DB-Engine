import java.util.*;

public class Table {
    private String tableName;
    private String primaryKey; // name of primary column
    private Vector<String> pagePaths;


    public Table(String tableName, String primaryKey) {
        this.tableName = tableName;
        pagePaths = new Vector<String>();
        this.primaryKey = primaryKey;
    }





}