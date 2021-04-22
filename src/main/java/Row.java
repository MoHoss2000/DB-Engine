import java.io.*;
import java.util.*;

public class Row implements Serializable, Comparable<Object> {
    private Hashtable<String, Object> rowData;
    private String primaryKey; // name of primary key col

    public Row(Hashtable<String, Object> rowData, String primaryKey) {
        this.rowData = rowData;
        this.primaryKey = primaryKey;
    }

    public Comparable<Object> getPrimaryKeyValue() {
        return (Comparable<Object>) rowData.get(primaryKey);
    }

    @Override
    public int compareTo(Object o) {
        Row rowToCompare = (Row) o;

        Comparable c1 = this.getPrimaryKeyValue();
        Comparable c2 = rowToCompare.getPrimaryKeyValue();

        return c1.compareTo(c2);
    }
}