import java.util.*;

public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm() {
    }

    static String alphabet = "0123456789-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    static String enbase(int x) {
        int n = alphabet.length();
//        int n = 123;


        if (x < n)
//            return (char) x + "";
            return alphabet.charAt(x) + "";

//        return enbase(x / n) + (char)(x % n);

        return enbase(x / n) + alphabet.charAt(x % n);
    }

    static int debase(String x) {
        int n = alphabet.length();
//        int n = 123;
        int result = 0;

        String reversed = new StringBuilder(x).reverse().toString();

        for (int i = 0; i < reversed.length(); i++) {
            char c = reversed.charAt(i);
            result += alphabet.indexOf(c) * (Math.pow(n, i));
//            result += (char) c * (Math.pow(n, i));

        }

        return result;
    }

    static String average(String a, String b) {
        int x = debase(a);

        int y = debase(b);

        return enbase((x + y) / 2);
    }

    public static void main(String[] args) {
        Integer[] sa= {1,2,3,4,5,6};

//        List<Integer> termsEvaluationList = Arrays.asList(sa);
//        System.out.println(termsEvaluationList);

        List<Integer> list = new LinkedList<Integer>(Arrays.asList(sa));


//        list.remove(0);
        list.add(0, 10);

        System.out.println(list);

        Hashtable rowData = new Hashtable();
        rowData.put("id", 1);
        rowData.put("age", 15.5);
        rowData.put("name", "mo");

        Row row = new Row(rowData, "id");

        System.out.println(row);

    }

}
