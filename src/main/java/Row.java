import java.io.*;
import java.util.*;

public class Row {
    Hashtable<String, Object> rowData;

    public Row(Hashtable<String, Object> rowData, String primaryKey) {
        this.rowData = rowData;
    }

    public static void main(String[] args) throws IOException{
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