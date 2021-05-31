import java.util.Hashtable;

public class RandomTests {
    public void createTable() {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        try {
            dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
        } catch (DBAppException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void insertIntoTable(){
        DBApp dbApp = new DBApp();
        dbApp.init();
        String tableName = "students";

        Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
        colNameValue.put("id", "46-9261");
        colNameValue.put("first_name", "dadad");

        try {
            dbApp.insertIntoTable("students", colNameValue);
        } catch (DBAppException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            dbApp.deleteFromTable(tableName, colNameValue);
        } catch (DBAppException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }
}
