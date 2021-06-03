import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket implements Serializable {
    private final Vector<Hashtable<String, Object>> keys;
//    private final String bucketPath;

    public Bucket() {
//        this.bucketPath = bucketPath;
        keys = new Vector<>();
    }

    public void addKey(PageData pageData, int rowIndexInPage){
        Hashtable<String, Object> rowRef = new Hashtable<String, Object>();
        rowRef.put("pageRef", pageData);
        rowRef.put("rowRef", rowIndexInPage);
        keys.add(rowRef);
    }

}
