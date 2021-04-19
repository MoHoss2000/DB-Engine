import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static String filePath = "src/main/resources/metadata.csv";
    private static String[] acceptableDataTypes = { "java.lang.Integer", "java.lang.String", "java.lang.Double",
            "java.util.Date" };

    ArrayList<Table> tablesInfo;

    @Override
    public void init() {

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

        // Creating a File object
        File tableFolderDir = new File("src/main/tables/" + tableName); // setting the path of the new folder
        // Creating the directory
        tableFolderDir.mkdir();
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        File tableFolderDir = new File("src/main/tables/" + tableName);
        String[] pagesList = tableFolderDir.list();

        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        Table table = new Table(tableName);

        String row;
        BufferedReader csvReader;

        try {
            csvReader = new BufferedReader(new FileReader(filePath));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvTableName = data[0];

                if (csvTableName == tableName) {
                    String colName = data[1];
                    String colType = data[2];
                    boolean isPrimary = Boolean.parseBoolean(data[3]);
                    boolean isIndexed = Boolean.parseBoolean(data[4]);

                    Object minValue = null;
                    Object maxValue = null;

                    switch (colType) {
                    case "java.lang.Integer":
                        minValue = Integer.parseInt(data[5]);
                        maxValue = Integer.parseInt(data[6]);
                        break;
                    case "java.lang.Double":
                        minValue = Double.parseDouble(data[5]);
                        maxValue = Double.parseDouble(data[6]);
                        break;
                    case "java.lang.String":
                        minValue = data[5];
                        maxValue = data[6];
                        break;
                    case "java.lang.Date":
                        minValue = Date.parse(data[5]);
                        maxValue = Date.parse(data[6]);
                        break;
                    }

                    Column column = new Column(colName, colType, minValue, maxValue, isIndexed, isPrimary);
                    table.addColumn(column);

                    if(isPrimary)
                        table.setPrimaryKey(column);
                }

            }

            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }



        if (pagesList.length == 0) { // inserting for 1st time - create page
            Page newPage = new Page();
            String primaryKey = table.primaryKey.colName;
            Row rowData = new Row(colNameValue, primaryKey);

            newPage.addRow(rowData);
            

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
        DBApp app = new DBApp();
        // app.checkName("");
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0");

        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        htblColNameMax.put("id", "100");
        htblColNameMax.put("name", "ZZZ");
        htblColNameMax.put("gpa", "999");

        try {
            app.createTable("Students", "id", htblColNameType, htblColNameMin, htblColNameMax);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

    }
}
