import java.io.FileOutputStream;
import java.io.*;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Vector;

public class PageData implements Serializable, Comparable {
    private String pagePath;
    private Comparable minKey;
    private Comparable maxKey;
    private int noOfRows;
    private Vector<PageData> overflowPagesData;

    public PageData(String pagePath, Comparable minKey, Comparable maxKey, int noOfRows) {
        this.pagePath = pagePath;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.noOfRows = noOfRows;
        overflowPagesData = new Vector<PageData>();
    }

    public int getNoOfRows() {
        return noOfRows;
    }

    public void incrementRows() {
        noOfRows++;
    }

    public void decrementRows() {
        noOfRows--;
    }

    public String getPagePath() {
        return pagePath;
    }

    public Comparable getMinKey() {
        return minKey;
    }

    public Comparable getMaxKey() {
        return maxKey;
    }

    public void setMinKey(Comparable key) {
        minKey = key;
    }

    public void setMaxKey(Comparable key) {
        maxKey = key;
    }

    public void addNewOverflowPage(Row row){
        Page newPage = new Page();
        newPage.addRow(row);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        long fileName = timestamp.getTime();

        String pagePath = "src/main/resources/data/"+ fileName + ".class";

        PageData pageData = new PageData(pagePath, row.getPrimaryKeyValue(), row.getPrimaryKeyValue(), 1);
        overflowPagesData.add(pageData);

        serializeObject(newPage, pagePath);
    }

    public Vector<PageData> getOverflowPagesData(){
        return overflowPagesData;
    }

    public void setOverflowPagesData(Vector<PageData> overflows){
        overflowPagesData = overflows;
    }

    @Override
    public int compareTo(Object o) {
        PageData pageToCompare = (PageData) o;
        return minKey.compareTo(pageToCompare.maxKey);
 
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

    public static void main(String[] args) {
        // PageData p1 = new PageData("dada", 2, 4, 2);
        // PageData p2 = new PageData("dada", 5, 8, 2);
        // PageData p3 = new PageData("dada", 10, 15, 2);
        // PageData p4 = new PageData("dada", 20, 25, 2);
        // PageData p5 = new PageData("dada", 30, 35, 2);

        // Vector pagesInfo = new Vector<>();
        // pagesInfo.add(p2);
        // pagesInfo.add(p3);
        // pagesInfo.add(p1);
        // pagesInfo.add(p4);
        // pagesInfo.add(p5);

        // Collections.sort(pagesInfo);

        // // System.out.println(pagesInfo.indexOf(p4));
        // Table t = new Table("tablename", "id");

        // PageData data = new PageData("da", 1, maxKe, noOfRows)

    }
}
