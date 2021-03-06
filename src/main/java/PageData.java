import java.io.FileOutputStream;
import java.io.*;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Vector;

public class PageData implements Serializable, Comparable {
    private final String pagePath;
    private Comparable minKey;
    private Comparable maxKey;
    private int noOfRows;

    public PageData(String pagePath, Comparable minKey, Comparable maxKey, int noOfRows) {
        this.pagePath = pagePath;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.noOfRows = noOfRows;
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


    @Override
    public int compareTo(Object o) {
        PageData pageToCompare = (PageData) o;
        return minKey.compareTo(pageToCompare.maxKey);
 
    }

    @Override
    public String toString(){
        return minKey + " " + maxKey + " " + noOfRows;
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
         PageData p1 = new PageData("dada", 1, 11, 4);
         PageData p2 = new PageData("dada", 12, 15, 4);
        // PageData p3 = new PageData("dada", 10, 15, 2);
        // PageData p4 = new PageData("dada", 20, 25, 2);
        // PageData p5 = new PageData("dada", 30, 35, 2);

         Vector pagesInfo = new Vector<>();
         pagesInfo.add(p1);
         pagesInfo.add(p2);
        // pagesInfo.add(p1);
        // pagesInfo.add(p4);
        // pagesInfo.add(p5);

         Collections.sort(pagesInfo);

//          System.out.println(pagesInfo.indexOf(p4));
         Table t = new Table("tablename", "id");
        t.insertPage(p1);
        t.insertPage(p2);


    }
}
