import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static final String filePath = "src/main/resources/metadata.csv";
    private static final String[] acceptableDataTypes = {"java.lang.Integer", "java.lang.String", "java.lang.Double",
            "java.util.Date"};
    private static final String[] operatorsInsideTerm = {">", ">=", "<", "<=", "!=", "="};
    private static final String[] operatorsBetweenTerms = {"OR", "AND", "XOR"};

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

        File tableFolderDir = new File("src/main/resources/data/" + tableName);

        // Creating the directory
        tableFolderDir.mkdir();

        Table newTable = new Table(tableName, clusteringKey);
        serializeObject(newTable, newTable.getFolderPath() + "/info.class");
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        String tablePath = "src/main/resources/data/" + tableName + "/info.class";
        Table table = (Table) deserializeFile(tablePath);
        List<String> colNamesList = Arrays.asList(columnNames);

        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        String row;
        BufferedReader csvReader;

        ArrayList<String> tableColumns = new ArrayList<String>();
        Comparable[] minValues = new Comparable[columnNames.length];
        Comparable[] maxValues = new Comparable[columnNames.length];

        try {
            csvReader = new BufferedReader(new FileReader(filePath));

            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvTableName = data[0];
                String colName = data[1];
                String colType = data[2];

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
                    int index = colNamesList.indexOf(colName);

                    if (index != -1) {
                        minValues[index] = minValue;
                        maxValues[index] = maxValue;
                    }
                }
            }


            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        for (String colName : columnNames
        ) {
            if (!tableColumns.contains(colName))
                throw new DBAppException("Invalid column name");
        }

        Vector<GIndex> indices = table.getIndices();

        // checking if index with same columns already exists
        boolean flag = true;
        for (GIndex index : indices
        ) {
            String[] indexColNames = index.getColNames();
            if (indexColNames.length == columnNames.length) {
                for (int i = 0; i < indexColNames.length; i++) {
                    if (!indexColNames[i].equals(columnNames[i])) {
                        flag = false;
                        break;
                    }
                }
            } else {
                flag = false;
                break;
            }


        }

//        if (flag)
//            throw new DBAppException("Index with these column names already exists");

        table.addIndex(columnNames, minValues, maxValues);
        serializeObject(table, tablePath);

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
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return result;
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        String tablePath = "src/main/resources/data/" + tableName + "/info.class";
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
//                System.out.println(row);
                String[] data = row.split(",");
                String csvTableName = data[0];
                String colName = data[1];
                String colType = data[2];
                boolean isPrimary = Boolean.parseBoolean(data[3]);
//                System.out.println("col: " + colName);

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
                            throw new DBAppException("One or more column not within the valid range");
                        }
                    }
                }
            }

            csvReader.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        Enumeration<String> enumeration = colNameValue.keys();

        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            if (!tableColumns.contains(key))
                throw new DBAppException("Invalid column name");
        }

        Row newRow = new Row(colNameValue, primaryKey);

        if (table.getNoOfPages() == 0) { // inserting for 1st time - create page
            File pagesFolderDir = new File(table.getFolderPath() + "/pages");

            // Creating the directory
            pagesFolderDir.mkdir();
            table.addPage(newRow);

            serializeObject(table, "src/main/resources/data/" + tableName + "/info.class");
        } else {
            PageData pageData = table.getPageForKey(newRow.getPrimaryKeyValue());
            String pagePath = pageData.getPagePath();
            Page page = (Page) deserializeFile(pagePath);

            if (page.binarySearchInPage(newRow) >= 0) {
                // page already has the value
                throw new DBAppException("Duplicate primary key");
            }


            // the page i want to insert in is not full
            if (pageData.getNoOfRows() < maxNoOfRows) {
                page.addRow(newRow);
                insertRowIntoAllIndices(table, newRow, page, pageData);

                pageData.incrementRows();
                pageData.setMinKey(page.getMinValue());
                pageData.setMaxKey(page.getMaxValue());

                serializeObject(page, pagePath);
                serializeObject(table, tablePath);
                return;
            }

            Vector<PageData> pagesInfo = table.getPagesInfo();
            int indexOfNextPageData = pagesInfo.indexOf(pageData) + 1;

            // i want to insert into the last page and it is full
            // i need to create a new page at the end
            if (indexOfNextPageData == pagesInfo.size()) {
                Comparable maxInOldPage = pageData.getMaxKey();
                if (newRow.getPrimaryKeyValue().compareTo(maxInOldPage) < 0) {
                    // key i want to insert is less than the max key in the old page
                    // i need to shift the max key to the new page

                    Row lastRow = page.removeLastRow(); // old max
                    deleteRowFromAllIndices(table, lastRow, page, pageData);

                    page.addRow(newRow);
                    insertRowIntoAllIndices(table, newRow, page, pageData);

                    pageData.setMaxKey(page.getMaxValue());
                    pageData.setMinKey(page.getMinValue());

                    table.addPage(lastRow);


                    serializeObject(page, pageData.getPagePath());
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
                    Row lastRow = page.removeLastRow();
                    deleteRowFromAllIndices(table, lastRow, page, pageData);


                    page.addRow(newRow);
                    insertRowIntoAllIndices(table, newRow, page, pageData);


                    pageData.setMaxKey(page.getMaxValue());
                    pageData.setMinKey(page.getMinValue());
                    serializeObject(page, pageData.getPagePath());

                    page = (Page) deserializeFile(nextPageData.getPagePath()); // next page

                    page.addRow(lastRow);
                    insertRowIntoAllIndices(table, lastRow, page, nextPageData);


                    nextPageData.setMaxKey(page.getMaxValue());
                    nextPageData.setMinKey(page.getMinValue());
                    nextPageData.incrementRows();

                    serializeObject(page, nextPageData.getPagePath());
                } else {
                    page = (Page) deserializeFile(nextPageData.getPagePath()); // next page
                    page.addRow(newRow);
                    insertRowIntoAllIndices(table, newRow, page, nextPageData);


                    nextPageData.setMaxKey(page.getMaxValue());
                    nextPageData.setMinKey(page.getMinValue());
                    nextPageData.incrementRows();
                    serializeObject(page, nextPageData.getPagePath());
                }

                serializeObject(table, tablePath);
                return;
            }

            // we will create a new page after the page we need to insert in
            Comparable maxInOldPage = pageData.getMaxKey();
            if (newRow.getPrimaryKeyValue().compareTo(maxInOldPage) < 0) {
                // key i want to insert is less than the max key in the old page
                // i need to shift the max key to the new page

                Row lastRow = page.removeLastRow(); // old max
                deleteRowFromAllIndices(table, lastRow, page, pageData);

                page.addRow(newRow);
                insertRowIntoAllIndices(table, newRow, page, pageData);

                pageData.setMaxKey(page.getMaxValue());
                pageData.setMinKey(page.getMinValue());

                table.addPage(lastRow);

                serializeObject(page, pageData.getPagePath());
            } else {
                table.addPage(newRow);
            }

            serializeObject(table, tablePath);
            return;
        }
    }

    public static void insertRowIntoAllIndices(Table table, Row row, Page page, PageData pageData) {
        int rowIndex = page.getRowIndex(row);

        Vector<GIndex> tableIndices = table.getIndices();
        for (GIndex index : tableIndices
        ) {
            index.insertKeyIntoGIndex(row, rowIndex, pageData);
        }
    }

    public static void deleteRowFromAllIndices(Table table, Row row, Page page, PageData pageData) {
        int rowIndex = page.getRowIndex(row);

        Vector<GIndex> tableIndices = table.getIndices();
        for (GIndex index : tableIndices
        ) {
            index.deleteKeyFromGIndex(row, pageData, rowIndex);
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {

        String tablePath = "src/main/resources/data/" + tableName + "/info.class";
        Table table = (Table) deserializeFile(tablePath);

        if (!checkName(tableName))
            throw new DBAppException("Table not found aslan!");

        String row;
        BufferedReader csvReader;

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

        String tablePath = "src/main/resources/data/" + tableName + "/info.class";
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

            if (rowIndex >= 0) {
                // page has the primary key
                Row rowToDelete = page.getRow(rowIndex);

                deleteRowFromMainPage(page, pageData, table, columnNameValue, rowIndex);
                deleteRowFromAllIndices(table, rowToDelete, page, pageData);
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
                        deleteRowFromMainPage(page, pageData, table, columnNameValue, j);

                        deleteRowFromAllIndices(table, tempRow, page, pageData);

                    }
                }
            }

        }
        table.serializeObject(table, tablePath);
    }

    public void deleteRowFromMainPage(Page page, PageData pageData, Table table, Hashtable columnNameValue,
                                      int rowIndex) {
        Row rowWithMatchingPrimary = page.getRow(rowIndex);

        if (checkAllColumns(rowWithMatchingPrimary, columnNameValue)) {
            page.deleteRow(rowIndex);
            pageData.decrementRows();

            if (pageData.getNoOfRows() == 0) {
                table.deletePage(pageData);
                return;
            }

            pageData.setMinKey(page.getMinValue());
            pageData.setMaxKey(page.getMaxValue());

            serializeObject(page, pageData.getPagePath());
            return;
        }
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
        if (sqlTerms.length != arrayOperators.length + 1)
            throw new DBAppException("Invalid Data Entry");

        ArrayList resultSet = new ArrayList();

        List<String> operatorsInside = Arrays.asList(operatorsInsideTerm);
        List<String> operatorsBetween = Arrays.asList(operatorsBetweenTerms);

        String tableName = sqlTerms[0]._strTableName;
        if (!checkName(tableName))
            throw new DBAppException("Table name not found aslan!");

        String tablePath = "src/main/resources/data/" + tableName + "/info.class";
        Table table = (Table) deserializeFile(tablePath);

        boolean allANDs = true;

        for (String operator : arrayOperators
        ) {
            if (!operatorsBetween.contains(operator))
                throw new DBAppException("Invalid operator");

            if (!operator.equals("AND")) {
                allANDs = false;
                break;
            }
        }

        String[] colNames = new String[sqlTerms.length];
        String primaryCol = "";

        for (int i = 0; i < sqlTerms.length; i++) {
            SQLTerm sqlTerm = sqlTerms[i];
            if (!sqlTerm._strTableName.equals(tableName))
                throw new DBAppException("Engine doesn't support joins!");

            if (!operatorsInside.contains(sqlTerm._strOperator))
                throw new DBAppException("Invalid operator");

            colNames[i] = sqlTerm._strColumnName;

            if (sqlTerm._strColumnName.equals(table.getPrimaryKeyCol())) {
                primaryCol = sqlTerm._strColumnName;
            }
        }

        if (allANDs) {
            Vector<GIndex> tableIndices = table.getIndices();
            for (int i = 0; i < tableIndices.size(); i++) {
                GIndex index = tableIndices.get(i);

                if (indexYenfa3Ma3Columns(colNames, index)) {
                    // use index to search
                }
            }

            if (!primaryCol.equals("")) {
                // binary search
            }
        }


        return resultSet.iterator();
    }

    public boolean indexYenfa3Ma3Columns(String[] colNames, GIndex index) {
        List<String> indexColNames = Arrays.asList(index.getColNames());

        for (String colName : colNames
        ) {
            if (!indexColNames.contains(colName)) {
                return false;
            }
        }

        return true;
    }

    public static void main(String[] args) throws IOException {
        DBApp dbApp = new DBApp();
        dbApp.init();

        Table table = (Table) dbApp.deserializeFile("src/main/tables/pcs.class");
        Vector<PageData> pagesData = table.getPagesInfo();
        int pageNo = 1;
        System.out.println("page " + "min" + " max" + " rows");
        for (PageData pageData : pagesData
        ) {
            System.out.println("  " + pageNo++ + " " + pageData);
        }
    }
}
