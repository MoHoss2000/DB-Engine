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

    public int getNoOfPages() {
        return pagesInfo.size();
    }

    public PageData getPageForInsertion(Comparable primaryKey) {
        int noOfPages = getNoOfPages();

        Comparable minKeys[] = new Comparable[noOfPages];

        for (int i = 0; i < noOfPages; i++) {
            minKeys[i] = pagesInfo.get(i).getMinKey();
        }

        int index = binarySearch(minKeys, 0, noOfPages - 1, primaryKey);
     
        if (index == -1)
        return null;

        return pagesInfo.get(index);
    }

    int binarySearch(Comparable arr[], int l, int r, Comparable x) {
        if (r >= l) {
            // 0 1
            int mid = l + (r - l) / 2;

            if (arr[mid].compareTo(x) == 0 || arr[mid + 1].compareTo(x) == 0)
                return -1;

            if (mid + 1 == arr.length)
                return mid;

            if (arr[mid].compareTo(x) < 0 && arr[mid + 1].compareTo(x) > 0)
                return mid;

            // 2 5 10 20 30
            // = 1

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (arr[mid].compareTo(x) > 0)
                return binarySearch(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            if (arr[mid].compareTo(x) < 0 && arr[mid + 1].compareTo(x) < 0)
                return binarySearch(arr, mid + 1, r, x);
        }

        return 0;
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

    public static void main(String[] args) {
        PageData p1 = new PageData("dada", 2, 4, 10);
        PageData p2 = new PageData("dada", 6, 8, 2);
        PageData p3 = new PageData("dada", 10, 15, 2);
        PageData p4 = new PageData("dada", 20, 24, 2);
        PageData p5 = new PageData("dada", 25, 27, 2);
        PageData p6 = new PageData("dada", 30, 35, 2);
        PageData p7 = new PageData("dada", 50, 70, 2);

        Vector pagesInfo = new Vector<>();
        pagesInfo.add(p2);
        pagesInfo.add(p3);
        pagesInfo.add(p1);
        pagesInfo.add(p4);
        pagesInfo.add(p5);
        pagesInfo.add(p6);
        pagesInfo.add(p7);

        Collections.sort(pagesInfo);

        // System.out.println(pagesInfo.indexOf(p4));
        Table t = new Table("tablename", "id");
        t.pagesInfo = pagesInfo;

        // System.out.println(t.getPageForInsertion(10));
        System.out.println(t.getPageForInsertion(1));

        // System.out.println;
    }
}