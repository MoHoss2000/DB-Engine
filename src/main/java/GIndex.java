import java.io.*;
import java.sql.Timestamp;
import java.util.*;

public class GIndex implements Serializable {
    private String tablePath;
    private Object[] gridIndex; // n-d array
    private String[] colNames; // list of column names
    private Comparable[] minValues; // respective min values for each column
    private Comparable[] maxValues; // -----------max values

    public GIndex(String[] colNames, Comparable[] minValues, Comparable[] maxValues, String tablePath) {
        this.colNames = colNames;
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.tablePath = tablePath;
        gridIndex = (Object[]) createMultDimArray(colNames.length, 10);
    }

    public String[] getColNames() {
        return colNames;
    }

    public static Object createMultDimArray(int dim, int size) {
        if (dim == 0) {
            return new Vector<PageData>();
        } else {
            Object[] array = new Object[size];
            for (int i = 0; i < size; i++) {
                array[i] = createMultDimArray(dim - 1, size);
            }

            return array;
        }
    }

    public static int findPosInDivisions(Comparable[] divisions, Comparable value) {
        for (int i = 0; i < divisions.length; i++) {
            if (i + 1 == divisions.length)
                return i;

            if (value.compareTo(divisions[i]) >= 0 && value.compareTo(divisions[i + 1]) < 0)
                return i;
        }

        return -1;
    }

    public void deleteKeyFromGIndex(Row row, PageData pageData, int rowIndex) {
        int[] arrPositions = new int[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            Comparable colMin = minValues[i];
            Comparable colMax = maxValues[i];

            Comparable[] divisions = getDivisions(colMin, colMax);
            Comparable colValue = row.getValueForCol(colNames[i]);

            arrPositions[i] = findPosInDivisions(divisions, colValue);
        }

        Object[] x = gridIndex;
        for (int i = 0; i < arrPositions.length; i++) {
            int indexInDiv = arrPositions[i];

            Vector<PageData> bucketsData;

            // reached the actual cells (last dim)
            if (i == arrPositions.length - 1) {
                bucketsData = (Vector<PageData>) x[indexInDiv];

                for (int j = 0; j < bucketsData.size(); j++) {
                    PageData bucketData = bucketsData.get(j);

                    Bucket bucket = (Bucket) deserializeFile(bucketData.getPagePath());

                    if (bucket.deleteKey(pageData, rowIndex)) {
                        bucketData.decrementRows();

                        if (bucketData.getNoOfRows() == 0) {
                            bucketsData.remove(bucketData);
                            File bucketFile = new File(bucketData.getPagePath());
                            bucketFile.delete();
                            return;
                        }

                        serializeObject(bucket, bucketData.getPagePath());
                        return;
                    }
                }
            } else
                // access the next dim
                x = (Object[]) x[indexInDiv];


        }


    }

    public void insertKeyIntoGIndex(Row row, int rowIndexInPage, PageData pageData) {
        int[] arrPositions = new int[colNames.length];

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
        int maxNoOfKeys = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket")); // max key limit

        for (int i = 0; i < colNames.length; i++) {
            Comparable colMin = minValues[i];
            Comparable colMax = maxValues[i];

            Comparable[] divisions = getDivisions(colMin, colMax);
            Comparable colValue = row.getValueForCol(colNames[i]);

            arrPositions[i] = findPosInDivisions(divisions, colValue);
        }

        Object[] x = gridIndex;
        for (int i = 0; i < arrPositions.length; i++) {
            int indexInDiv = arrPositions[i];

            Vector<PageData> bucketsData;

            // reached the actual cells (last dim)
            if (i == arrPositions.length - 1) {
                bucketsData = (Vector<PageData>) x[indexInDiv];

                if (bucketsData.isEmpty()) {
                    // inserting first key -> add first bucket
                    File bucketsFolder = new File(tablePath + "/buckets");
                    bucketsFolder.mkdir();

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    long bucketFileName = timestamp.getTime();

                    String bucketPath = tablePath + "/buckets/" + bucketFileName + ".class";

                    Bucket bucket = new Bucket();
                    bucket.addKey(pageData, rowIndexInPage);

                    PageData bucketData = new PageData(bucketPath, null, null, 1);
                    bucketsData.add(bucketData);

                    serializeObject(bucket, bucketPath);
                    return;
                }

                for (int j = 0; j < bucketsData.size(); j++) {
                    PageData bucketData = bucketsData.get(j);
                    if (bucketData.getNoOfRows() >= maxNoOfKeys) {

                        if (j == bucketsData.size() - 1) {
                            // create new bucket
                            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                            long bucketFileName = timestamp.getTime();

                            String bucketPath = tablePath + "/buckets/" + bucketFileName + ".class";

                            Bucket bucket = new Bucket();
                            bucket.addKey(pageData, rowIndexInPage);

                            PageData newBucketData = new PageData(bucketPath, null, null, 1);
                            bucketsData.add(newBucketData);

                            serializeObject(bucket, bucketPath);
                            return;
                        }

                    } else {
                        // deserialize the old bucket and add the key to it
                        Bucket bucket = (Bucket) deserializeFile(bucketData.getPagePath());
                        bucket.addKey(pageData, rowIndexInPage);

                        bucketData.incrementRows();

                        serializeObject(bucket, bucketData.getPagePath());

                        return;

                    }

                }
            } else
                // access the next dim
                x = (Object[]) x[indexInDiv];


//            System.out.println(Arrays.toString(x));
//            System.out.println(indexInDiv);


        }


    }

    public static Comparable[] getDivisions(Comparable min, Comparable max) {
        Comparable[] divisions = new Comparable[10];
        switch (min.getClass().getName()) {
            case "java.lang.Double":
                double minDouble = (double) min;
                double maxDouble = (double) max;

                double doubleDivision = (maxDouble - minDouble) / 10;
                double doubleCounter = minDouble;

                for (int i = 0; i < 10; i++) {
                    divisions[i] = doubleCounter;
                    doubleCounter += doubleDivision;
                }
                break;
            case "java.lang.Integer":
                int minValue = (Integer) min;
                int maxValue = (Integer) max;

                int intDivision = (int) Math.round(((double) (maxValue - minValue)) / 10);
                int intCounter = minValue;

                for (int i = 0; i < 10; i++) {
                    divisions[i] = intCounter;
                    intCounter += intDivision;
                }
                break;
            case "java.util.Date":
                long minStamp = ((Date) min).getTime();
                long maxStamp = ((Date) max).getTime();

                long dateDivision = ((maxStamp - minStamp) / 10);
                long dateCounter = minStamp;

                for (int i = 0; i < 10; i++) {
                    Calendar c = Calendar.getInstance();

                    c.setTimeInMillis(dateCounter);
                    dateCounter += dateDivision;
                    divisions[i] = c.getTime();
                }
                break;
            default:
                // string
                for (int i = 0; i < 10; i++) {

                    divisions[i] = "a";
                }
                break;
        }

        return divisions;
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

    public static void main(String[] args) {
//        Comparable[] divisions = getDivisions(1, 100);
//        System.out.println(Arrays.toString(divisions));
//        System.out.println(findPosInDivisions(divisions, 1));

        Hashtable<String, Object> rowData = new Hashtable<String, Object>();
        rowData.put("id", 5);
        rowData.put("name", "Mido");
        rowData.put("age", 20);
        rowData.put("gpa", 2.5);

        String[] colNames = {"id", "gpa"};
        Comparable[] minValues = {1, 0.1};
        Comparable[] maxValues = {100, 5.0};

//        GIndex index = new GIndex(colNames, minValues, maxValues);
        Row row = new Row(rowData, "id");
//        index.insertKeyIntoGIndex(row);
    }
}
