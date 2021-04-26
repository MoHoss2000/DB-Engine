import java.io.BufferedReader;
import java.io.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static String filePath = "src/main/resources/metadata.csv";
    private static String[] acceptableDataTypes = { "java.lang.Integer", "java.lang.String", "java.lang.Double",
            "java.util.Date" };
    private static int maxNoOfRows;

    ArrayList<Table> tablesInfo;

    @Override
    public void init() {
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
        maxNoOfRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage")); // max row limit

        File directory = new File("src/main/tables");
        if (!directory.exists()) {
            directory.mkdir();
        }

        directory = new File("src/main/resources/data");
        if (!directory.exists()) {
            directory.mkdir();
        }
        
    }

    public void writeInCSV(ArrayList<Object> list) throws IOException {
        FileWriter csvWriter = new FileWriter(filePath, true); // true for append mode

        for (int i = 0; i < list.size(); i++) {
            String value = list.get(i).toString();
            csvWriter.append(value);
            csvWriter.append(",");
        }

        csvWriter.append("\n");

        csvWriter.flush();
        csvWriter.close();
    }

    public static boolean checkName(String name) {
        // Input file which needs to be parsed
        BufferedReader fileReader = null;

        // Delimiter used in CSV file
        final String DELIMITER = ",";
        try {
            String line = "";
            // Create the file reader
            fileReader = new BufferedReader(new FileReader(filePath));

            // Read the file line by line
            while ((line = fileReader.readLine()) != null) {
                // Get all tokens available in line
                String[] tokens = line.split(DELIMITER);

                String tableName = tokens[0];
                if (name.equals(tableName))
                    return true; // if tables exists

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return false;
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        // Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max
        Iterator<String> iterator = colNameType.keys().asIterator();
        ArrayList<String> colNames = new ArrayList<String>();

        while (iterator.hasNext()) {
            colNames.add(iterator.next());
        }

        boolean alreadyExists = checkName(tableName);

        if (alreadyExists)
            throw new DBAppException("Name already exists");

        boolean isValidPrimaryKey = false;
        for (int i = 0; i < colNames.size(); i++) {

            List<String> dataTypes = Arrays.asList(acceptableDataTypes);
            String colName = colNames.get(i);
            String colType = colNameType.get(colName);

            if (colName.equals(clusteringKey))
                isValidPrimaryKey = true;

            if (!dataTypes.contains(colType))
                throw new DBAppException("Invalid data type");
        }

        if (!isValidPrimaryKey)
            throw new DBAppException("Invalid primary key");

        for (int i = 0; i < colNameType.size(); i++) {
            ArrayList<Object> columnInfo = new ArrayList<Object>();

            String colName = colNames.get(i);

            // System.out.println(colName);

            String colMin = colNameMin.get(colName);
            String colMax = colNameMax.get(colName);

            String colType = colNameType.get(colName);

            columnInfo.add(tableName);
            columnInfo.add(colName); // column name
            columnInfo.add(colType); // column type
            columnInfo.add(colName == clusteringKey); // is primary key?
            columnInfo.add(false); // is indexed?
            columnInfo.add(colMin); // min value
            columnInfo.add(colMax); // max value

            try {
                writeInCSV(columnInfo);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // File tableFolderDir = new File("src/main/pages/" + tableName); // setting the
        // path of the new folder
        // // Creating the directory
        // tableFolderDir.mkdir();

        Table newTable = new Table(tableName, clusteringKey);
        serializeObject(newTable, "src/main/tables/" + tableName + ".class");
    }

    public void serializeObject(Serializable object, String path) {
        try {
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(object);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Object deserializeFile(String path) {

        Object result = null;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            result = in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
        } catch (ClassNotFoundException c) {
            c.printStackTrace();
        }

        return result;
    }

    // x, y, z == table columns
    // x,y,z,a == column i want to insert
    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        String tablePath = "src/main/tables/" + tableName + ".class";
        Table table = (Table) deserializeFile(tablePath);

        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        String row;
        BufferedReader csvReader;
        String primaryKey = null;

        ArrayList<String> tableColumns = new ArrayList<String>();
        try {
            csvReader = new BufferedReader(new FileReader(filePath));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvTableName = data[0];
                String colName = data[1];
                String colType = data[2];
                boolean isPrimary = Boolean.parseBoolean(data[3]);

                Comparable minValue = null;
                Comparable maxValue = null;

                switch (colType) {
                case "java.lang.Double":
                    minValue = Double.parseDouble(data[5]);
                    maxValue = Double.parseDouble(data[6]);
                    break;
                case "java.lang.Integer":
                    minValue = Integer.parseInt(data[5]);
                    maxValue = Integer.parseInt(data[6]);
                    break;
                case "java.util.Date":
                    minValue = new SimpleDateFormat("yyyy-MM-dd").parse(data[5]);
                    maxValue = new SimpleDateFormat("yyyy-MM-dd").parse(data[6]);
                    break;
                default:
                    minValue = data[5];
                    maxValue = data[6];
                }

                if (csvTableName.equals(tableName)) { // checking table name
                    tableColumns.add(colName);

                    if (isPrimary)
                        primaryKey = colName;

                    if (isPrimary && !colNameValue.containsKey(colName))
                        throw new DBAppException("Primary key must be inserted");

                    if (!colNameValue.containsKey(colName))
                        colNameValue.put(colName, null);

                    if (colNameValue.containsKey(colName)) {
                        if (!colType.equals(colNameValue.get(colName).getClass().getName()))
                            throw new DBAppException("Invalid data types!");

                        Comparable colValue = (Comparable) colNameValue.get(colName);

                        if (minValue.compareTo(colValue) > 0 || maxValue.compareTo(colValue) < 0) {

                            // System.out.println("min: " + minValue + " " + " max :" + maxValue + " value:
                            // " + colValue + colValue.getClass().getName());
                            // throw new DBAppException("One or more column not within the valid range");
                        }
                    }
                }
            }

            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        Enumeration<String> enumeration = colNameValue.keys();
        // col names that the user inserted

        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            if (!tableColumns.contains(key))
                throw new DBAppException("Invalid column name");

        }

        Row newRow = new Row(colNameValue, primaryKey);

        if (table.getNoOfPages() == 0) { // inserting for 1st time - create page
            table.addPage(newRow);
            serializeObject(table, "src/main/tables/" + tableName + ".class");
        } else {
            PageData pageData = table.getPageForKey(newRow.getPrimaryKeyValue());
            String pagePath = pageData.getPagePath();
            Page page = (Page) deserializeFile(pagePath);

            if (page.binarySearchInPage(newRow) >= 0) {
                // page already has the value
                // throw new DBAppException("Duplicate primary key");
            }

            Vector<PageData> overflowPages = pageData.getOverflowPagesData();

            if (overflowPages.size() > 0) {
                for (int i = 0; i < overflowPages.size(); i++) {
                    PageData overflow = pageData.getOverflowPagesData().get(i);
                    page = (Page) deserializeFile(overflow.getPagePath());
                    // if (page.binarySearchInPage(newRow) >= 0)
                    // throw new DBAppException("Duplicate primary key");
                }
            }

            // the page i want to insert in is not full
            if (pageData.getNoOfRows() < maxNoOfRows) {
                page = (Page) deserializeFile(pagePath);

                page.addRow(newRow);
                pageData.incrementRows();
                pageData.setMinKey(page.getMinValue());
                pageData.setMaxKey(page.getMaxValue());

                serializeObject(page, pagePath);
                serializeObject(table, tablePath);
                return;
            }

            Vector<PageData> pagesInfo = table.getPagesInfo();
            int indexOfNextPageData = pagesInfo.indexOf(pageData) + 1;

            if (indexOfNextPageData == pagesInfo.size()) { // i need to create a new page
                Comparable maxInOldPage = pageData.getMaxKey();
                if (newRow.getPrimaryKeyValue().compareTo(maxInOldPage) < 0) {
                    // key i want to insert is less than the max key in the old page
                    // i need to shift the max key to the new page
                    Page oldPage = (Page) deserializeFile(pageData.getPagePath());
                    Row lastRow = oldPage.removeLastRow();

                    oldPage.addRow(newRow);

                    pageData.setMaxKey(oldPage.getMaxValue());
                    pageData.setMinKey(oldPage.getMinValue());

                    table.addPage(lastRow);

                    serializeObject(oldPage, pageData.getPagePath());
                } else {
                    table.addPage(newRow);
                }

                serializeObject(table, tablePath);
                return;
            }

            PageData nextPageData = pagesInfo.elementAt(indexOfNextPageData);

            if (nextPageData.getNoOfRows() < maxNoOfRows) {
                Comparable maxInOldPage = pageData.getMaxKey();
                if (newRow.getPrimaryKeyValue().compareTo(maxInOldPage) < 0) {
                    Page loadedPage = (Page) deserializeFile(pageData.getPagePath()); // old page
                    Row lastRow = loadedPage.removeLastRow();

                    loadedPage.addRow(newRow);

                    pageData.setMaxKey(loadedPage.getMaxValue());
                    pageData.setMinKey(loadedPage.getMinValue());
                    serializeObject(loadedPage, pageData.getPagePath());

                    loadedPage = (Page) deserializeFile(nextPageData.getPagePath()); // next page

                    loadedPage.addRow(lastRow);
                    nextPageData.setMaxKey(loadedPage.getMaxValue());
                    nextPageData.setMinKey(loadedPage.getMinValue());
                    nextPageData.incrementRows();

                    serializeObject(loadedPage, nextPageData.getPagePath());
                } else {
                    Page loadedPage = (Page) deserializeFile(nextPageData.getPagePath()); // old page
                    loadedPage.addRow(newRow);

                    nextPageData.setMaxKey(loadedPage.getMaxValue());
                    nextPageData.setMinKey(loadedPage.getMinValue());
                    nextPageData.incrementRows();
                    serializeObject(loadedPage, nextPageData.getPagePath());
                }

                serializeObject(table, tablePath);
                return;
            }

            // we need to create overflow page linked to the original page i want to insert
            // in

            Vector<PageData> overflowPagesData = pageData.getOverflowPagesData();

            for (int i = 0; i < overflowPagesData.size(); i++) {
                PageData currentOverflow = overflowPagesData.get(i);
                if (currentOverflow.getNoOfRows() < maxNoOfRows) {
                    Page loadedPage = (Page) deserializeFile(currentOverflow.getPagePath()); // old page
                    loadedPage.addRow(newRow);

                    currentOverflow.setMaxKey(loadedPage.getMaxValue());
                    currentOverflow.setMinKey(loadedPage.getMinValue());

                    serializeObject(loadedPage, currentOverflow.getPagePath());
                    serializeObject(table, tablePath);
                    return;
                }
            }

            pageData.addNewOverflowPage(newRow); // creating new overflow
            serializeObject(table, tablePath);
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {

        String tablePath = "src/main/tables/" + tableName + ".class";
        Table table = (Table) deserializeFile(tablePath);

        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        String row;
        BufferedReader csvReader;
        // String primaryKey = null;

        ArrayList<String> tableColumns = new ArrayList<String>();
        Comparable primaryKeyValue = null;

        try {
            csvReader = new BufferedReader(new FileReader(filePath));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvTableName = data[0];
                String colName = data[1];
                String colType = data[2];
                boolean isPrimary = Boolean.parseBoolean(data[3]);

                Comparable minValue = null;
                Comparable maxValue = null;

                switch (colType) {
                case "java.lang.Double":
                    if (isPrimary && csvTableName.equals(tableName))
                        primaryKeyValue = Double.parseDouble(clusteringKeyValue);
                    minValue = Double.parseDouble(data[5]);
                    maxValue = Double.parseDouble(data[6]);
                    break;
                case "java.lang.Integer":
                    if (isPrimary && csvTableName.equals(tableName))
                        primaryKeyValue = Integer.parseInt(clusteringKeyValue);
                    minValue = Integer.parseInt(data[5]);
                    maxValue = Integer.parseInt(data[6]);
                    break;
                case "java.util.Date":
                    if (isPrimary && csvTableName.equals(tableName))
                        primaryKeyValue = new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
                    minValue = new SimpleDateFormat("yyyy-MM-dd").parse(data[5]);
                    maxValue = new SimpleDateFormat("yyyy-MM-dd").parse(data[6]);
                    break;
                default:
                    if (isPrimary && csvTableName.equals(tableName))
                        primaryKeyValue = clusteringKeyValue;
                    minValue = data[5];
                    maxValue = data[6];
                }

                if (csvTableName.equals(tableName)) { // checking table name
                    tableColumns.add(colName);

                    if (columnNameValue.containsKey(colName)) {
                        if (!colType.equals(columnNameValue.get(colName).getClass().getName()))
                            throw new DBAppException("Invalid data types!");

                        Comparable colValue = (Comparable) columnNameValue.get(colName);

                        if (minValue.compareTo(colValue) > 0 || maxValue.compareTo(colValue) < 0) {
                            // System.out.println("min: " + minValue + " " + " max :" + maxValue + " value:
                            // " + colValue + colValue.getClass().getName());
                            // throw new DBAppException("One or more column not within the valid range");
                        }
                    }
                }
            }

            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        Enumeration<String> enumeration = columnNameValue.keys();
        // col names that the user inserted

        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            if (!tableColumns.contains(key))
                throw new DBAppException("Invalid column name");

        }

        PageData pageData = table.getPageForKey(primaryKeyValue);
        Page page = (Page) deserializeFile(pageData.getPagePath());

        Hashtable<String, Object> dummyRowHashtable = new Hashtable<>();
        dummyRowHashtable.put(table.getPrimaryKeyCol(), primaryKeyValue);
        Row dummyRow = new Row(dummyRowHashtable, table.getPrimaryKeyCol());

        int rowIndex = page.binarySearchInPage(dummyRow);

        Row rowToUpdate = null;

        if (rowIndex >= 0) {
            // row is found in main page
            rowToUpdate = page.getRow(rowIndex);
        } else {
            Vector<PageData> overflowPages = pageData.getOverflowPagesData();

            for (int i = 0; i < overflowPages.size(); i++) {
                PageData overflowData = overflowPages.get(i);
                page = (Page) deserializeFile(overflowData.getPagePath());

                rowIndex = page.binarySearchInPage(dummyRow);

                if (rowIndex >= 0) {
                    rowToUpdate = page.getRow(rowIndex);
                    i = overflowPages.size(); // to end loop
                }
            }
        }

        if (rowToUpdate != null) {
            Enumeration<String> enumeration2 = columnNameValue.keys();

            while (enumeration2.hasMoreElements()) {
                String colName = enumeration2.nextElement();
                Comparable requiredColValue = (Comparable) columnNameValue.get(colName);
                rowToUpdate.changeValueForCol(colName, requiredColValue);
            }
        } else {
            System.out.println("Column to update not found");
        }

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        String tablePath = "src/main/tables/" + tableName + ".class";
        Table table = (Table) deserializeFile(tablePath);

        String tablePrimaryCol = table.getPrimaryKeyCol(); // name of column

        String row;
        BufferedReader csvReader;

        ArrayList<String> tableColumns = new ArrayList<String>();
        try {
            csvReader = new BufferedReader(new FileReader(filePath));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvTableName = data[0];
                String colName = data[1];
                String colType = data[2];
                boolean isPrimary = Boolean.parseBoolean(data[3]);

                Comparable minValue = null;
                Comparable maxValue = null;

                switch (colType) {
                case "java.lang.Double":
                    minValue = Double.parseDouble(data[5]);
                    maxValue = Double.parseDouble(data[6]);
                    break;
                case "java.lang.Integer":
                    minValue = Integer.parseInt(data[5]);
                    maxValue = Integer.parseInt(data[6]);
                    break;
                case "java.util.Date":
                    minValue = new SimpleDateFormat("yyyy-MM-dd").parse(data[5]);
                    maxValue = new SimpleDateFormat("yyyy-MM-dd").parse(data[6]);
                    break;
                default:
                    minValue = data[5];
                    maxValue = data[6];
                }

                if (csvTableName.equals(tableName)) { // checking table name
                    tableColumns.add(colName);

                    if (columnNameValue.containsKey(colName)) {
                        if (!colType.equals(columnNameValue.get(colName).getClass().getName()))
                            throw new DBAppException("Invalid data types!");

                        Comparable colValue = (Comparable) columnNameValue.get(colName);

                        if (minValue.compareTo(colValue) > 0 || maxValue.compareTo(colValue) < 0) {

                            // System.out.println("min: " + minValue + " " + " max :" + maxValue + " value:
                            // " + colValue + colValue.getClass().getName());
                            throw new DBAppException("One or more column not within the valid range");
                        }
                    }
                }
            }

            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        Enumeration<String> enumeration = columnNameValue.keys();
        // col names that the user inserted

        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            if (!tableColumns.contains(key))
                throw new DBAppException("Invalid column name");
        }

        if (columnNameValue.contains(tablePrimaryCol)) {
            // binary search
            Comparable primaryKeyValue = (Comparable) columnNameValue.get(tablePrimaryCol);

            PageData pageData = table.getPageForKey(primaryKeyValue);
            Page page = (Page) deserializeFile(pageData.getPagePath());

            Hashtable<String, Object> dummyRowHashtable = new Hashtable<>();
            dummyRowHashtable.put(tablePrimaryCol, primaryKeyValue);
            Row dummyRow = new Row(dummyRowHashtable, tablePrimaryCol);

            int rowIndex = page.binarySearchInPage(dummyRow);
            Vector<PageData> overflowPages = pageData.getOverflowPagesData();

            if (rowIndex >= 0) {
                // page has the primary key
                // benemsa7 mn main page
                deleteRowFromMainPage(page, pageData, table, columnNameValue, rowIndex);
            }

            for (int i = 0; i < overflowPages.size(); i++) {
                PageData overflow = pageData.getOverflowPagesData().get(i);
                page = (Page) deserializeFile(overflow.getPagePath());
                if (page.binarySearchInPage(dummyRow) >= 0) {
                    // benemsa7 mn overflow
                    deleteRowFromOverflowPage(page, rowIndex, overflow, overflowPages);
                }
            }

        } else {
            // linear search
            Vector<PageData> pagesData = table.getPagesInfo();

            for (int i = 0; i < pagesData.size(); i++) {
                PageData pageData = pagesData.get(i);
                Page page = (Page) deserializeFile(pageData.getPagePath());

                for (int j = 0; j < pageData.getNoOfRows(); j++) {
                    Row tempRow = page.getRow(j);
                    if (checkAllColumns(tempRow, columnNameValue)) {
                        // emsa7 el row mn main page
                        deleteRowFromMainPage(page, pageData, table, columnNameValue, j);
                    }
                }

                Vector<PageData> overflowPages = pageData.getOverflowPagesData();

                for (int j = 0; j < overflowPages.size(); j++) {
                    PageData overflowPageData = overflowPages.get(j);
                    page = (Page) deserializeFile(overflowPageData.getPagePath());

                    for (int k = 0; k < overflowPageData.getNoOfRows(); k++) {
                        Row tempRow = page.getRow(j);
                        if (checkAllColumns(tempRow, columnNameValue)) {
                            // emsa7 el row mn overflow
                            deleteRowFromOverflowPage(page, k, overflowPageData, overflowPages);
                        }
                    }
                }
            }

        }

    }

    public void deleteRowFromMainPage(Page page, PageData pageData, Table table, Hashtable columnNameValue,
            int rowIndex) {
        Vector<PageData> overflowPages = pageData.getOverflowPagesData();
        Row rowWithMatchingPrimary = page.getRow(rowIndex);

        if (checkAllColumns(rowWithMatchingPrimary, columnNameValue)) {
            page.deleteRow(rowIndex);
            pageData.decrementRows();

            if (pageData.getNoOfRows() == 0) {

                if (overflowPages.size() > 0) {
                    // die with inheritence
                    PageData walyEl3ahd = overflowPages.get(0);
                    overflowPages.remove(walyEl3ahd);
                    walyEl3ahd.setOverflowPagesData(overflowPages);

                    table.deletePage(pageData);
                    table.insertPage(walyEl3ahd);

                } else
                    // die peacfully
                    table.deletePage(pageData);

                return;
            }

            pageData.setMinKey(page.getMinValue());
            pageData.setMaxKey(page.getMaxValue());
            return;
        }
    }

    public void deleteRowFromOverflowPage(Page page, int rowIndex, PageData overflow, Vector<PageData> overflowPages) {
        page.deleteRow(rowIndex);
        overflow.decrementRows();
        if (overflow.getNoOfRows() == 0) {
            // benemsa7 el overflow
            overflowPages.remove(overflow);
            File overFlowFile = new File(overflow.getPagePath());
            overFlowFile.delete();
            return;
        }
        overflow.setMinKey(page.getMinValue());
        overflow.setMaxKey(page.getMaxValue());
        return;
    }

    public boolean checkAllColumns(Row row, Hashtable<String, Object> hashtable) {
        Enumeration<String> enumeration = hashtable.keys();

        while (enumeration.hasMoreElements()) {
            String colName = enumeration.nextElement();
            Comparable requiredColValue = (Comparable) hashtable.get(colName);
            Comparable rowColValue = row.getValueForCol(colName);
            if (requiredColValue.compareTo(rowColValue) != 0)
                return false;
        }

        return true;
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        // TODO Auto-generated method stub
        return null;
    }

    public static void main(String[] args) throws IOException {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String tableName = "students";

        // Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        // htblColNameType.put("id", "java.lang.String");
        // htblColNameType.put("first_name", "java.lang.String");
        // htblColNameType.put("last_name", "java.lang.String");
        // htblColNameType.put("dob", "java.util.Date");
        // htblColNameType.put("gpa", "java.lang.Double");

        // Hashtable<String, String> minValues = new Hashtable<>();
        // minValues.put("id", "43-0000");
        // minValues.put("first_name", "AAAAAA");
        // minValues.put("last_name", "AAAAAA");
        // minValues.put("dob", "1990-01-01");
        // minValues.put("gpa", "0.7");

        // Hashtable<String, String> maxValues = new Hashtable<>();
        // maxValues.put("id", "99-9999");
        // maxValues.put("first_name", "zzzzzz");
        // maxValues.put("last_name", "zzzzzz");
        // maxValues.put("dob", "2000-12-31");
        // maxValues.put("gpa", "5.0");

        // try {
        // dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
        // } catch (DBAppException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
        colNameValue.put("id", 11);
        colNameValue.put("gpa", 0.9);

        try {
            dbApp.insertIntoTable("students", colNameValue);
        } catch (DBAppException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
