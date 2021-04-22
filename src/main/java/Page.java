import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

public class Page implements Serializable {
    private int pageNo;
    private Vector<Row> pageRows;

    public Page() {
        pageRows = new Vector<Row>();
    }

    public Comparable getMinValue() {
        Row minRow = pageRows.get(0);
        return minRow.getPrimaryKeyValue();
    }

    public Comparable getMaxValue() {
        Row maxRow = pageRows.lastElement();
        return maxRow.getPrimaryKeyValue();
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
}
