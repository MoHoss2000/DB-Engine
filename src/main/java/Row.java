import java.io.*;
import java.util.*;

public class Row implements Serializable, Comparable<Object> {
    private Hashtable<String, Object> rowData;
    private String primaryKey; // name of primary key col

    public Comparable<Object> getPrimaryKeyValue(){
        return (Comparable<Object>) rowData.get(primaryKey);
    }

    public Row(Hashtable<String, Object> rowData, String primaryKey) {
        this.rowData = rowData;
        this.primaryKey = primaryKey;

    }

    @Override
    public int compareTo(Object o) {
        Row rowToCompare = (Row) o;

        Comparable c1 = this.getPrimaryKeyValue();
        Comparable c2 = rowToCompare.getPrimaryKeyValue();

        return c1.compareTo(c2);
    }

    public static void main(String[] args) throws IOException {
        String row;
        String filePath = "src/main/resources/metadata.csv";

        BufferedReader csvReader = new BufferedReader(new FileReader(filePath));
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(",");

            System.out.println(data[0] + " " + data[1] + " " + data[2]);
        }
        csvReader.close();
    }
}