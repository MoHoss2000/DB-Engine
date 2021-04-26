import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Page implements Serializable {
    private static final long serialVersionUID = 2529685098267757690L;
    private Vector<Row> pageRows;

    public Page() {
        pageRows = new Vector<Row>();
    }

    public int binarySearchInPage(Row key) {
        return Collections.binarySearch(pageRows, key);
    }

    public Comparable getMinValue() {
        Row minRow = pageRows.get(0);
        return minRow.getPrimaryKeyValue();
    }

    public Comparable getMaxValue() {
        Row maxRow = pageRows.lastElement();
        return maxRow.getPrimaryKeyValue();
    }

    public Row removeLastRow() {
        return pageRows.remove(pageRows.size() - 1);
    }

    public boolean checkIfPrimaryKeyExists(Comparable primaryKey) {
        boolean exists = false;
        for (Row row : pageRows) {
            Comparable rowPrimaryKey = row.getPrimaryKeyValue();
            if (rowPrimaryKey.compareTo(primaryKey) == 0)
                exists = true;
        }

        return exists;
    }

    public void addRow(Row row) {
        pageRows.add(row);
        Collections.sort(pageRows);
    }

    public void deleteRow(int index){
        pageRows.remove(index);
    }

    public Row getRow(int index){
        return pageRows.get(index);
    }

    public boolean isFull() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {

        }
        try {
            prop.load(is);
        } catch (IOException ex) {

        }
        int maxCount = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage")); // max row limit
        int rowCount = pageRows.size(); // actual rows in page

        return rowCount == maxCount;
    }

    public static void main(String[] args) {
        // Vector<Integer> vector = new Vector<Integer>();
        // vector.add(1);
        // vector.add(2);
        // vector.add(3);

        // vector.remove(Integer.valueOf(2));

        // System.out.println(vector);

        // 100 - 2000
        // 511
        Comparable minValue = "100";
        Comparable maxValue = "2000";
        Comparable colValue = "511";

        if (minValue.compareTo(colValue) > 0 || maxValue.compareTo(colValue) < 0) {

            System.out.println("error");
            // throw new DBAppException("One or more column not within the valid range");
        }

        try {
            Date d = new SimpleDateFormat("yyyy-MM-dd").parse("2000-04-07");
            System.out.println(d);

        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
