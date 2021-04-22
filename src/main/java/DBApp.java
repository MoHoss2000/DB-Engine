import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

        // Table table = new Table(tableName);

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

        if (pagesList.length == 0) { // inserting for 1st time - create page
            Page newPage = new Page();

            Row newRow = new Row(colNameValue, primaryKey);
            newPage.addRow(newRow);

            try {
                FileOutputStream fileOut = new FileOutputStream("src/main/tables/" + tableName + "/1.class");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(newPage);
                out.close();
                fileOut.close();
                // System.out.printf("Serialized data is saved in /tmp/employee.ser");
            } catch (IOException i) {
                i.printStackTrace();
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
        DBApp app = new DBApp();

        Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
        colNameValue.put("id", 4);
        colNameValue.put("gpa", 0.9);

        try {
            app.insertIntoTable("students", colNameValue);
        } catch (DBAppException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
