package storage;

import java.io.*;
import java.util.*;

import utils.*;

public class DavisBaseBinaryFile {

  public static boolean showRowId = false;
  public static boolean dataStoreInitialized = false;
  public static String davisbaseColumns = "davisbase_columns";
  public static String davisbaseTables = "davisbase_tables";

  /* This makes sure that Page size is always a power of 2, default value: 512 */
  static int pageSize = (int) Math.pow(2, Constants.PAGE_SIZE_POWER);

  RandomAccessFile file;

  public DavisBaseBinaryFile(RandomAccessFile file) {
    this.file = file;
  }

  /* to check if a record with the condition exists in the table */
  public boolean recordExists(TableMetaData tablemetaData, Condition condition) throws IOException {
    BPlusOneTree bPlusOneTree = new BPlusOneTree(file, tablemetaData.rootPageNum, tablemetaData.tableName);
    for (Integer pageNum : bPlusOneTree.getAllLeaves(condition)) {
      Page page = new Page(file, pageNum);

      // loop through records and find records that match the condition
      for (TableRecord record : page.getPageRecords()) {
        if (condition != null) {
          if (!condition.checkCondition(record.getAttributes().get(condition.columnOrdinal).fieldValue))
            continue;
        }

        return true;
      }
    }

    return false;
  }

  /* update the records that match the mentioned condition and returns count */
  public int updateRecords(TableMetaData tablemetaData, Condition condition,
      List<String> columNames, List<String> newValues) throws IOException {
    int count = 0;

    List<Integer> ordinalPositions = tablemetaData.getOrdinalPositions(columNames);

    // to map new values to ordinal column positions
    int j = 0;
    Map<Integer, Attribute> newValueMap = new HashMap<>();

    // add attributes for each new value
    for (String newValue : newValues) {
      int index = ordinalPositions.get(j);

      try {
        newValueMap.put(index,
            new Attribute(tablemetaData.colNameAttributes.get(index).dataType, newValue));
      } catch (Exception e) {
        Utils.log("! Invalid data format for " + tablemetaData.colNames.get(index) + " values: "
            + newValue);
        return count;
      }

      j++;
    }

    BPlusOneTree bPlusOneTree = new BPlusOneTree(file, tablemetaData.rootPageNum, tablemetaData.tableName);

    // loop through each page and check the record that matches update condition
    for (Integer pageNo : bPlusOneTree.getAllLeaves(condition)) {
      short deleteCountPerPage = 0;
      Page page = new Page(file, pageNo);

      // loop through each page record to check if it matches the condition
      for (TableRecord record : page.getPageRecords()) {
        if (condition != null) {
          if (!condition.checkCondition(record.getAttributes().get(condition.columnOrdinal).fieldValue)) {
            continue;
          }
        }

        count++;

        // for each new value to be updated check for delete possibility
        for (int i : newValueMap.keySet()) {
          Attribute oldValue = record.getAttributes().get(i);
          int rowId = record.rowId;

          if ((record.getAttributes().get(i).dataType == DataType.TEXT
              && record.getAttributes().get(i).fieldValue.length() == newValueMap.get(i).fieldValue.length())
              || (record.getAttributes().get(i).dataType != DataType.NULL
                  && record.getAttributes().get(i).dataType != DataType.TEXT)) {
            page.updateRecord(record, i, newValueMap.get(i).fieldValueByte);
          } else {
            page.DeleteTableRecord(tablemetaData.tableName,
                Integer.valueOf(record.pageHeaderIndex - deleteCountPerPage).shortValue());

            deleteCountPerPage++;

            List<Attribute> attrs = record.getAttributes();
            Attribute attr;

            attrs.remove(i);
            attr = newValueMap.get(i);
            attrs.add(i, attr);

            rowId = page.addTableRow(tablemetaData.tableName, attrs);
          }

          if (tablemetaData.colNameAttributes.get(i).hasIndex && condition != null) {
            RandomAccessFile indexFile = new RandomAccessFile(Utils.getNDXFilePath(
                tablemetaData.colNameAttributes.get(i).tableName,
                tablemetaData.colNameAttributes.get(i).columnName),
                Constants.READ_WRITE_MODE);

            BTree bTree = new BTree(indexFile);

            bTree.delete(oldValue, record.rowId);
            bTree.insert(newValueMap.get(i), rowId);

            Utils.closeIOFile(indexFile);
          }
        }
      }
    }

    if (!tablemetaData.tableName.equals(davisbaseTables) && !tablemetaData.tableName.equals(davisbaseColumns)) {
      Utils.log("Record(s) are updated");
    }

    return count;
  }

  /* to select records from the table that matches the condition */
  public void selectRecords(TableMetaData tablemetaData, List<String> columNames, Condition condition)
      throws IOException {
    List<Integer> ordinalPositions = tablemetaData.getOrdinalPositions(columNames);

    Utils.log("");
    List<Integer> printPosition = new ArrayList<>();

    int columnPrintLength = 0;
    printPosition.add(columnPrintLength);

    int totalTablePrintLength = 0;

    if (showRowId) {
      System.out.print("rowid");
      System.out.print(Utils.line(" ", 5));
      printPosition.add(10);
      totalTablePrintLength += 10;
    }

    // loop through each ordinal position and get total printable length
    for (int i : ordinalPositions) {
      String columnName = tablemetaData.colNameAttributes.get(i).columnName;
      columnPrintLength = Math.max(columnName.length(),
          tablemetaData.colNameAttributes.get(i).dataType.getPrintOffset())
          + 5;
      printPosition.add(columnPrintLength);
      System.out.print(columnName);
      System.out.print(Utils.line(" ", columnPrintLength - columnName.length()));
      totalTablePrintLength += columnPrintLength;
    }

    Utils.log("");
    Utils.log(Utils.line("-", totalTablePrintLength));

    BPlusOneTree bPlusOneTree = new BPlusOneTree(file, tablemetaData.rootPageNum, tablemetaData.tableName);

    String currentValue;

    // loop through all the leaves of bPlusOne tree
    for (Integer pageNo : bPlusOneTree.getAllLeaves(condition)) {
      Page page = new Page(file, pageNo);

      // loop through each page record to check if it matches the condition
      for (TableRecord record : page.getPageRecords()) {
        if (condition != null) {
          if (!condition.checkCondition(record.getAttributes().get(condition.columnOrdinal).fieldValue))
            continue;
        }

        int columnCount = 0;

        if (showRowId) {
          currentValue = Integer.valueOf(record.rowId).toString();
          System.out.print(currentValue);
          System.out.print(Utils.line(" ", printPosition.get(++columnCount) - currentValue.length()));
        }

        // loop through each ordinal positions
        for (int i : ordinalPositions) {
          currentValue = record.getAttributes().get(i).fieldValue;
          System.out.print(currentValue);
          System.out.print(Utils.line(" ", printPosition.get(++columnCount) - currentValue.length()));
        }

        Utils.log("");
      }
    }

    Utils.log("");
  }

  // Find the root page manually
  public static int getRootPageNo(RandomAccessFile binFile) {
    int rPage = 0;

    try {
      // loop through each page
      for (int i = 0; i < binFile.length() / DavisBaseBinaryFile.pageSize; i++) {
        binFile.seek((long) i * DavisBaseBinaryFile.pageSize + 0x0A);
        int a = binFile.readInt();

        if (a == -1) {
          return i;
        }
      }

      return rPage;
    } catch (Exception e) {
      Utils.log(Constants.RecordOperations.FETCH_ROOT_PAGE_ERROR);
      Utils.log(e + "");
    }

    return -1;
  }

  /**
   * This static method creates the DavisBase data storage container and then
   * initializes two .tbl files to implement the two system tables,
   * davisbase_tables and davisbase_columns
   * <p>
   * WARNING! Calling this method will destroy the system database catalog files
   * if they already exist.
   */
  public static void initializeDataStore() {
    // Generate data container directory at the current OS location hold
    try {
      String[] oldTableFiles;
      File dataDir = new File("data");

      dataDir.mkdir();
      oldTableFiles = dataDir.list();

      assert oldTableFiles != null;

      // loop and remove old table files
      for (String oldTableFile : oldTableFiles) {
        File anOldFile = new File(dataDir, oldTableFile);
        anOldFile.delete();
      }
    } catch (SecurityException se) {
      Utils.log(Constants.RecordOperations.INIT_DATA_STORE_ERROR);
      Utils.log(se + "");
    }

    // generate davisbase_tables catalogs
    try {
      int currentPageNo = 0;

      RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
          Utils.getTBLFilePath(davisbaseTables), Constants.READ_WRITE_MODE);

      Page.addNewPage(davisbaseTablesCatalog, Page.PageType.LEAF, -1, -1);

      Page page = new Page(davisbaseTablesCatalog, currentPageNo);

      page.addTableRow(davisbaseTables, Arrays.asList(new Attribute(DataType.TEXT, DavisBaseBinaryFile.davisbaseTables),
          new Attribute(DataType.INT, "2"),
          new Attribute(DataType.SMALLINT, "0"),
          new Attribute(DataType.SMALLINT, "0")));

      page.addTableRow(davisbaseTables,
          Arrays.asList(new Attribute(DataType.TEXT, DavisBaseBinaryFile.davisbaseColumns),
              new Attribute(DataType.INT, "11"),
              new Attribute(DataType.SMALLINT, "0"),
              new Attribute(DataType.SMALLINT, "2")));

      Utils.closeIOFile(davisbaseTablesCatalog);
    } catch (Exception e) {
      Utils.log(Constants.RecordOperations.CREATE_TABLES_ERROR);
      Utils.log(e + "");
    }

    // generate davisbase_columns catalogs
    try {
      RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
          Utils.getTBLFilePath(davisbaseColumns), Constants.READ_WRITE_MODE);

      Page.addNewPage(davisbaseColumnsCatalog, Page.PageType.LEAF, -1, -1);

      Page page = new Page(davisbaseColumnsCatalog, 0);

      short ordinal_position = 1;

      page.addNewColumn(new ColumnInfo(davisbaseTables, DataType.TEXT, "table_name", true, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseTables, DataType.INT, "record_count", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseTables, DataType.SMALLINT, "avg_length", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseTables, DataType.SMALLINT, "root_page", false, false, ordinal_position++));

      ordinal_position = 1;

      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.TEXT, "table_name", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.TEXT, "column_name", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.SMALLINT, "data_type", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.SMALLINT, "ordinal_position", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.TEXT, "is_nullable", false, false, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.SMALLINT, "column_key", false, true, ordinal_position++));
      page.addNewColumn(
          new ColumnInfo(davisbaseColumns, DataType.SMALLINT, "is_unique", false, false, ordinal_position++));

      Utils.closeIOFile(davisbaseColumnsCatalog);
      dataStoreInitialized = true;
    } catch (Exception e) {
      Utils.log(Constants.RecordOperations.CREATE_COLUMNS_ERROR);
      Utils.log(e + "");
    }
  }
}
