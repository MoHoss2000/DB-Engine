public class Column {
    String colName;
    String colType;
    Object minValue;
    Object maxValue;
    boolean isIndexed;
    boolean isPrimary;

    public Column(String colName, String colType, Object minValue, Object maxValue, 
            boolean isIndexed,
            boolean isPrimary) {
        this.colName = colName;
        this.colType = colType;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.isIndexed = isIndexed;
        this.isPrimary = isPrimary;
    }

}
