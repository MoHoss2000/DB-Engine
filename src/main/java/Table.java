import java.io.FileOutputStream;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;

public class Table implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;

    private String tableName;
    private String primaryKey; // name of primary column
    private Vector<PageData> pagesInfo;
    

    public Table(String tableName, String primaryKey) {
        this.tableName = tableName;
        pagesInfo = new Vector<PageData>();
        this.primaryKey = primaryKey;
    }

    public int getNoOfPages(){
        return pagesInfo.size();
    }

    public void addPage(Row row) {
        Page newPage = new Page();
        newPage.addRow(row);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        long fileName = timestamp.getTime();

        String pagePath = "src/main/pages/" + tableName + "/" + fileName + ".class";

        PageData pageData = new PageData(pagePath, row.getPrimaryKeyValue(), row.getPrimaryKeyValue(), 1);
        pagesInfo.add(pageData);

        serializeObject(newPage, pagePath);        
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
}