import java.util.*;

public class Table  {
    String tableName;
    Column primaryKey;
    Vector<Column> columns;

    public Table(String tableName) {
        this.tableName = tableName;
        columns = new Vector<Column>();
    }

    public void addColumn(Column newCol){
        columns.add(newCol);
    }

    public void setPrimaryKey(Column primary){
        primaryKey = primary;
    }

}