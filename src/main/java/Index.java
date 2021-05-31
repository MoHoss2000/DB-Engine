import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

public class Index implements Serializable {
    private Object[] gridIndex;
    private String[] colNames;
    private Comparable[] minValues;

    public Index(String[] colNames, Comparable[] minValues, Comparable[] maxValues){
        this.colNames = colNames;
        gridIndex = createMultDimArray(colNames.length, 10);
    }

    public String[] getColNames(){
        return colNames;
    }

    public static Object[] createMultDimArray(int dim, int size){
        if(dim == 1){
            return null;
        } else{
            Object[] array = new Object[size];
            for (int i=0; i<size; i++){
                array[i] = createMultDimArray(dim-1, size);
            }

            return array;
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

        }

        return divisions;
    }
}
