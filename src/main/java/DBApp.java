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

            System.out.println(colName);

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

        File tableFolderDir = new File("src/main/pages/" + tableName); // setting the path of the new folder
        // Creating the directory
        tableFolderDir.mkdir();

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

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        String tablePath = "src/main/tables/" + tableName + ".class";
        Table table = (Table) deserializeFile(tableName);

        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        String row;
        BufferedReader csvReader;
        String primaryKey = null;

        try {
            csvReader = new BufferedReader(new FileReader(filePath));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvTableName = data[0];
                String colName = data[1];
                String colType = data[2];
                boolean isPrimary = Boolean.parseBoolean(data[3]);

                if (isPrimary)
                    primaryKey = colName;

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
                    minValue = new SimpleDateFormat("YYYY-MM-DD").parse(data[5]);
                    maxValue = new SimpleDateFormat("YYYY-MM-DD").parse(data[6]);
                    break;
                default:
                    minValue = data[5];
                    maxValue = data[6];
                }

                if (csvTableName == tableName) {
                    if (isPrimary && !colNameValue.containsKey(colName))
                        throw new DBAppException("Primary key must be inserted");

                    if (!colNameValue.containsKey(colName))
                        colNameValue.put(colName, null);

                    if (colNameValue.containsKey(colName)) {
                        if (!colType.equals(colNameValue.get(colName).getClass().getName()))
                            throw new DBAppException("Invalid data types!");

                        Comparable colValue = (Comparable) colNameValue.get(colName);

                        if (minValue.compareTo(colValue) > 0 || maxValue.compareTo(colValue) < 0) {
                            throw new DBAppException("One or more column not within the valid range");
                        }
                    }
                }
            }

            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        Row newRow = new Row(colNameValue, primaryKey);

        if (table.getNoOfPages() == 0) { // inserting for 1st time - create page
            table.addPage(newRow);
            serializeObject(table, "src/main/tables/" + tableName + ".class");
        } else {
            PageData pageData = table.getPageForInsertion(newRow.getPrimaryKeyValue());
            if(pageData == null)   
                throw new DBAppException("Duplicate primary key");


            // the page i want to insert in is not full
            if (pageData.getNoOfRows() < maxNoOfRows) { 
                String pagePath = pageData.getPagePath();
                Page page = (Page) deserializeFile(pagePath);
                
                page.addRow(newRow);
                pageData.incrementRows();
                pageData.setMinKey(page.getMinValue());
                pageData.setMaxKey(page.getMaxValue());

                serializeObject(page, pagePath);
                serializeObject(table, tablePath);
            }
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // TODO Auto-generated method stub

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        // TODO Auto-generated method stub
        return null;
    }

    public static void main(String[] args) throws IOException {
        DBApp dbApp = new DBApp();

        // String tableName = "students";

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

        // Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
        // colNameValue.put("id", 4);
        // colNameValue.put("gpa", 0.9);

        // try {
        // dbApp.insertIntoTable("students", colNameValue);
        // } catch (DBAppException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        // Table table = new Table("test", "id");
        // dbApp.serializeObject(table, "src/main/tables/test.class");

        // Table deserialized = (Table)
        // dbApp.deserializeFile("src/main/tables/test.class");
        // System.out.println(deserialized.getClass().getName());

    }
}
