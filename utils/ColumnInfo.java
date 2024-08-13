package utils;

import storage.DataType;
import java.io.*;

public class ColumnInfo {
  public boolean isUnique;
  public String tableName;
  public boolean isPrimaryKey;
  public boolean isNullable;
  public Short ordinalPosition;
  public boolean hasIndex;
  public DataType dataType;
  public String columnName;

  public ColumnInfo() {
  }

  public ColumnInfo(String tableName, String columnName, DataType dataType, boolean isNullable) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.tableName = tableName;
    this.isNullable = isNullable;
  }

  public ColumnInfo(String tableName, DataType dataType, String columnName,
      boolean isUnique, boolean isNullable, short ordinalPosition) {
    this.tableName = tableName;
    this.dataType = dataType;
    this.isNullable = isNullable;
    this.ordinalPosition = ordinalPosition;
    this.columnName = columnName;
    this.isUnique = isUnique;
    this.hasIndex = (new File(Utils.getNDXFilePath(tableName, columnName)).exists());
  }

  public void setAsPrimaryKey() {
    isPrimaryKey = true;
  }

}
