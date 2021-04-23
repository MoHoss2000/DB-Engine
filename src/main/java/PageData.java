import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

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

    public Comparable getMinKey() {
        return minKey;
    }

    public Comparable getMaxKey() {
        return maxKey;
    }
    
    @Override
    public int compareTo(Object o) {
        PageData pageToCompare = (PageData) o;

        return minKey.compareTo(pageToCompare.maxKey);
    }

    public static void main(String[] args) {
        PageData p1 = new PageData("dada", 1, 8, 8);
        PageData p2 = new PageData("dada", 9, 15, 3);

        System.out.println(p2.compareTo(p1));


    }
}
