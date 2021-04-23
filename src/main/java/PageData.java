import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

public class PageData implements Serializable, Comparable {
    private String pagePath;
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
        // if (o instanceof PageData) {
        PageData pageToCompare = (PageData) o;
        return minKey.compareTo(pageToCompare.maxKey);
        // } else {
        // Row row = (Row) o;
        // if(maxKey.compareTo(row) > 0 && minKey.compareTo(row) < 0) return 0;
        // else if(maxKey.compareTo(row) )
        // }
    }

    public static void main(String[] args) {
        PageData p1 = new PageData("dada", 2, 4, 2);
        PageData p2 = new PageData("dada", 5, 8, 2);
        PageData p3 = new PageData("dada", 10, 15, 2);
        PageData p4 = new PageData("dada", 20, 25, 2);
        PageData p5 = new PageData("dada", 30, 35, 2);

        Vector pagesInfo = new Vector<>();
        pagesInfo.add(p2);
        pagesInfo.add(p3);
        pagesInfo.add(p1);
        pagesInfo.add(p4);
        pagesInfo.add(p5);

        Collections.sort(pagesInfo);

        // System.out.println(pagesInfo.indexOf(p4));
        Table t = new Table("tablename", "id");

    }
}
