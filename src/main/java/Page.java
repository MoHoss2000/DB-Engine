
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Page implements Serializable {
    private static final long serialVersionUID = 2529685098267757690L;
    private final Vector<Row> pageRows;

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

    public void deleteRow(int index) {
        pageRows.remove(index);
    }

    public Row getRow(int index) {
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


    public static int getFirstDiffCharIndex(String a, String b) {
        for (int i = 0; i < a.length(); i++) {
            if (a.charAt(i) != b.charAt(i))
                return i;
        }

        return -1;
    }

    public static void main(String[] args) throws ParseException {
//        Date[] divisions = new Date[10];
//
//        Date minValue = new SimpleDateFormat("yyyy-MM-dd").parse("2000-04-07");
//        Date maxValue = new SimpleDateFormat("yyyy-MM-dd").parse("2020-05-04");
//
//        long minStamp = minValue.getTime();
//        long maxStamp = maxValue.getTime();
//
//        long division =  ((maxStamp - minStamp) / 10);
//        System.out.println(division);
//
//        long x = minStamp;
//
//
//        for (int i = 0; i < 10; i++) {
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//
//            Calendar c = Calendar.getInstance();
//
//            c.setTimeInMillis(x + division);
//            x += division;
//            divisions[i] = c.getTime();
////            divisions[i] = sdf.format(c.getTime());
//        }
//
//        System.out.println(Arrays.toString(divisions));


    }

}

