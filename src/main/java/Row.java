import java.io.*;
import java.util.*;

public class Row implements Serializable, Comparable<Object> {
    private final Hashtable<String, Object> rowData;
    private final String primaryKey; // name of primary key col

    public Row(Hashtable<String, Object> rowData, String primaryKey) {
        this.rowData = rowData;
        this.primaryKey = primaryKey;
    }

    public Comparable<Object> getPrimaryKeyValue() {
        return (Comparable<Object>) rowData.get(primaryKey);
    }

    public Comparable getValueForCol(String colName) {
        return (Comparable) rowData.get(colName);
    }

    // {"name": "menna", "id": 10, "gpa" : 0.7}
    public void changeValueForCol(String colName, Comparable value){
        rowData.put(colName, value);
    }

    @Override
    public int compareTo(Object o) {
        Row rowToCompare = (Row) o;

        Comparable c1 = this.getPrimaryKeyValue();
        Comparable c2 = rowToCompare.getPrimaryKeyValue();

        return c1.compareTo(c2);

    }

    public String toString(){
        String string = "";

        Enumeration<String> enumeration = rowData.keys();

        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            string += rowData.get(key) + ", ";
        }

        return string;
    }

    public static void main(String[] args) {
        Vector<Row> page = new Vector<Row>();

        Hashtable h1 = new Hashtable<>();
        Hashtable h2 = new Hashtable<>();
        Hashtable h3 = new Hashtable<>();

        h1.put("id", 10);
        h1.put("name", "Moh");

        h2.put("id", 5);
        h2.put("name", "Ali");

        h3.put("id", 20);
        h3.put("name", "xada");

        Row r1 = new Row(h1, "id");
        Row r2 = new Row(h2, "id");
        Row r3 = new Row(h3, "id");

        Hashtable h4 = new Hashtable<>();
        h4.put("id", 20);
        h4.put("name", "dadadadada");
        Row r4 = new Row(h4, "id");

        page.add(r1);
        page.add(r2);
        page.add(r3);

        Collections.sort(page);
        System.out.println(Collections.binarySearch(page, r4));
    }

}