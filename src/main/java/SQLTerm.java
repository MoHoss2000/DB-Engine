import java.util.Arrays;

public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm() {
    }



    public static void main(String[] args) {

        int[] dimensionLength = {6,6};

//        [ a-l , m-z ]
//        [ [a-l + 1-50  , a-l + 51-100 ], [ l-z + 1-50 ,  l-z + 51- 100 ]  ]
//        [ [ [ a-l+ 1-50 + 0-2.5 , a-l+1-50+2.5-5 ],[a-l+51-100+0-2.5 , a-l + 51-100 + 2.5-5 ] ] , [ [] ,[] ] ]


        int[][] table = new int[2][2];

        for(int i=0; i<table.length; i++){
            System.out.println( Arrays.toString(table[i]) );
        }

    }
}
