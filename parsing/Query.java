package parsing;

import java.io.*;
import java.util.*;

import storage.Attribute;
import storage.BPlusOneTree;
import storage.BTree;
import storage.DataType;
import storage.DavisBaseBinaryFile;
import storage.Page;

import utils.Constants.CreateIndex;
import utils.Constants.ParseCreateTable;
import utils.Constants.ParseDelete;
import utils.Constants.ParseInsert;
import utils.Constants.ParseQuery;
import utils.Constants.ParseUpdate;
import utils.Constants.ParseUserQuery;
import utils.ColumnInfo;
import utils.Condition;
import utils.Constants;
import utils.TableMetaData;
import utils.TableRecord;
import utils.Utils;

public class Query {

  public static void parseCreateIndex(String query) {
    // Query: CREATE INDEX ON <TABLE_NAME> (PRIMARY_KEY_ATTRIBUTE)

    List<String> createIndexTokens = Utils.getSpaceSeparatedTokens(query);

    try {
      if (!Utils.CreateIndexCommand.checkParseCreateIndexQuerySyntax(query,
          createIndexTokens)) {
        return;
      }

      String tableName = Utils.CreateIndexCommand.getTableNameFromQuery(query);
      String columnName = Utils.CreateIndexCommand.getColumnNameFromQuery(query);

      if (Utils.CreateIndexCommand.checkIfIndexExists(tableName, columnName)) {
        return;
      }

      RandomAccessFile tableFile = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_WRITE_MODE);
      TableMetaData metaData = new TableMetaData(tableName);

      if (!Utils.CreateIndexCommand.isTableValid(tableFile, metaData)) {
        return;
      }

      if (!Utils.CreateIndexCommand.isTableValid(tableFile, metaData)) {
        return;
      }

      int columnOrdinal = Utils.getColumnOrdinalPosition(metaData, columnName);

      if (!Utils.CreateIndexCommand.isColumnNameValid(tableFile, columnOrdinal)) {
        return;
      }

      RandomAccessFile indexFile = new RandomAccessFile(Utils.getNDXFilePath(tableName, columnName),
          Constants.READ_WRITE_MODE);

      Page.addNewPage(indexFile, Page.PageType.LEAFINDEX, -1, -1);

      if (metaData.recordCount > 0) {
        BPlusOneTree bPlusOneTree = new BPlusOneTree(tableFile, metaData.rootPageNum, metaData.tableName);

        for (int pageNo : bPlusOneTree.getAllLeaves()) {
          Page page = new Page(tableFile, pageNo);
          BTree bTree = new BTree(indexFile);

          for (TableRecord record : page.getPageRecords()) {
            bTree.insert(record.getAttributes().get(columnOrdinal), record.rowId);
          }
        }
      }

      Utils.log(CreateIndex.INDEX_CREATED + columnName);

      Utils.closeIOFile(indexFile);
      Utils.closeIOFile(tableFile);
    } catch (IOException e) {
      Utils.log(CreateIndex.INDEX_CREATION_ERROR);
      Utils.log(e + "");
    }
  }

  public static void dropTable(String query) {
    if (!Utils.DropCommand.checkDropCommandSyntax(Utils.getSpaceSeparatedArray(query))) {
      return;
    }

    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    String tableName = queryTokens.get(2);

    String deleteTablesQueryStr = Utils.DropCommand.generateDropDeleteQuery(tableName,
        DavisBaseBinaryFile.davisbaseTables);

    String deleteColumnsQueryStr = Utils.DropCommand.generateDropDeleteQuery(tableName,
        DavisBaseBinaryFile.davisbaseColumns);

    parseDelete(deleteTablesQueryStr);
    parseDelete(deleteColumnsQueryStr);

    Utils.DropCommand.deleteTableFile(tableName);
    Utils.DropCommand.deleteIndexes(tableName);
  }

  // method to parse SELECT user query command
  public static void parseQuery(String query) {
    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    List<List<String>> res = Utils.ParseQuery.parseTableAndColNames(queryTokens);

    String tableName = res.get(0).get(0);
    List<String> columnNames = res.get(1);

    TableMetaData tableMetaData = new TableMetaData(tableName);

    if (!Utils.ParseQuery.isTableValid(tableMetaData, tableName)) {
      return;
    }

    Condition condition;
    try {
      condition = Utils.QueryCondition.fetchAndParseQueryCondition(tableMetaData, query);
    } catch (Exception e) {
      Utils.log(e.getMessage());
      return;
    }

    boolean requestedAllColumns = Utils.ParseQuery.areAllColumnsRequested(columnNames);

    if (requestedAllColumns) {
      // assign all columns of the table
      columnNames = tableMetaData.colNames;
    }

    try {
      RandomAccessFile tableFile = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_MODE);

      DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(tableFile);

      Utils.ParseQuery.fetchRecordsBasedOnCondition(tableBinaryFile, tableMetaData, condition, columnNames);

      Utils.closeIOFile(tableFile);
    } catch (IOException exception) {
      Utils.log(ParseQuery.DATA_ERROR);
    }
  }

  // method to parse UPDATE user query command
  public static void parseUpdate(String query) {
    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    String tableName = Utils.UpdateCommand.getTableNameFromTokens(queryTokens);

    if (!Utils.UpdateCommand.checkUpdateQuerySyntax(queryTokens)) {
      return;
    }

    List<List<String>> populatedResult = Utils.UpdateCommand.populateTargetColumnsAndValues(query);

    List<String> columnsToUpdate = populatedResult.get(0);
    List<String> valueToUpdate = populatedResult.get(1);

    TableMetaData metaData = new TableMetaData(tableName);

    if (!Utils.UpdateCommand.isTableNameValid(metaData)) {
      return;
    }

    if (!Utils.UpdateCommand.areColumnNamesValid(columnsToUpdate, metaData)) {
      return;
    }

    Condition condition;
    try {
      condition = Utils.QueryCondition.fetchAndParseQueryCondition(metaData, query);
    } catch (Exception e) {
      Utils.log(e.getMessage());
      return;
    }

    try {
      RandomAccessFile file = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_WRITE_MODE);

      DavisBaseBinaryFile binaryFile = new DavisBaseBinaryFile(file);

      int noOfRecordsUpdated = Utils.UpdateCommand.getRecordsCountTobeUpdated(columnsToUpdate, valueToUpdate, condition,
          binaryFile, metaData);

      if (noOfRecordsUpdated > 0) {
        List<Integer> allRowids = new ArrayList<>();

        for (ColumnInfo colInfo : metaData.colNameAttributes) {
          int i = 0;

          while (i < columnsToUpdate.size()) {
            if (colInfo.columnName.equals(columnsToUpdate.get(i)) && colInfo.hasIndex) {
              if (condition == null) {
                // deleting index file

                if (allRowids.size() == 0) {
                  BPlusOneTree bPlusOneTree = new BPlusOneTree(file, metaData.rootPageNum, metaData.tableName);

                  for (int pageNo : bPlusOneTree.getAllLeaves()) {
                    Page currentPage = new Page(file, pageNo);

                    for (TableRecord record : currentPage.getPageRecords()) {
                      allRowids.add(record.rowId);
                    }
                  }
                }

                RandomAccessFile indexFile = new RandomAccessFile(
                    Utils.getNDXFilePath(tableName, columnsToUpdate.get(i)),
                    Constants.READ_WRITE_MODE);

                Page.addNewPage(indexFile, Page.PageType.LEAFINDEX, -1, -1);

                BTree bTree = new BTree(indexFile);

                Attribute attr_ = new Attribute(colInfo.dataType, valueToUpdate.get(i));

                bTree.insert(attr_, allRowids);
              }
            }

            i++;
          }
        }
      }

      Utils.closeIOFile(file);
    } catch (Exception e) {
      Utils.log(ParseUpdate.TABLE_UPDATE_ERROR);
      Utils.log(e + "");
    }
  }

  // method to parse INSERT user query command
  public static void parseInsert(String query) {
    // Query: INSERT INTO table_name ( columns ) VALUES ( values );

    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    if (!Utils.InsertCommand.checkInsertQuerySyntax(query, queryTokens)) {
      return;
    }

    try {
      String tableName = Utils.InsertCommand.getTableNameFromTokens(queryTokens);

      if (!Utils.InsertCommand.checkForEmptyTableName(tableName)) {
        return;
      }

      tableName = Utils.InsertCommand.parseTableNameFromQuery(tableName);

      TableMetaData metaData = new TableMetaData(tableName);

      if (!Utils.InsertCommand.isTableValid(metaData)) {
        return;
      }

      List<String> columnTokens = Utils.InsertCommand.getColumnTokens(query);

      if (!Utils.InsertCommand.areColumnsValid(columnTokens, metaData)) {
        return;
      }

      List<String> valueTokens = Utils.InsertCommand.getParsedValuesTokens(Utils.InsertCommand.getParsedValues(query));

      List<Attribute> attributeToInsert = new ArrayList<>();

      for (ColumnInfo colInfo : metaData.colNameAttributes) {
        boolean columnProvided = false;
        int i = 0;

        while (i < columnTokens.size()) {
          if (Utils.removeLeadingTrailingWhitespaces(columnTokens.get(i)).equals(colInfo.columnName)) {
            columnProvided = true;

            try {
              String value = Utils.removeLeadingTrailingWhitespaces(Utils.removeQuotesFromString(valueTokens.get(i)));

              if (Utils.removeLeadingTrailingWhitespaces(valueTokens.get(i)).equals(Constants.NULL_STRING)) {
                if (!colInfo.isNullable) {
                  Utils.log(ParseInsert.NULL_INSERTION_ERROR + colInfo.columnName);
                  return;
                }

                colInfo.dataType = DataType.NULL;
                value = value.toUpperCase();
              }

              Attribute attr = new Attribute(colInfo.dataType, value);
              attributeToInsert.add(attr);

              break;
            } catch (Exception e) {
              Utils.log(ParseInsert.DATA_ERROR_1 + columnTokens.get(i) + ParseInsert.DATA_ERROR_2
                  + valueTokens.get(i));
              return;
            }
          }

          i++;
        }

        Utils.InsertCommand.removeExcessTokens(i, columnTokens, valueTokens);

        if (!Utils.InsertCommand.handleIfColNotPresentInQuery(columnProvided, colInfo, attributeToInsert)) {
          return;
        }
      }

      RandomAccessFile tableFile = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_WRITE_MODE);

      int pageNum = BPlusOneTree.getPageNumForInsertion(tableFile, metaData.rootPageNum);

      Page page = new Page(tableFile, pageNum);

      int rowNum = page.addTableRow(tableName, attributeToInsert);

      Utils.InsertCommand.updateColumnIndexes(metaData, tableName, attributeToInsert, rowNum);

      Utils.closeIOFile(tableFile);

      if (rowNum != -1) {
        Utils.log(ParseInsert.INSERTION_SUCCESS);
      }
    } catch (Exception ex) {
      Utils.log(ParseInsert.INSERTION_ERROR);
      Utils.log(ex + "");
    }
  }

  // method to parse CREATE TABLE user query command
  public static void parseCreateTable(String query) {
    // Query: CREATE TABLE <TABLE_NAME> (<ATTRIBUTE> <DATA_TYPE> <PRIMARY KEY>,
    // ...);

    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    if (!Utils.CreateTable.checkCreateCommandSyntax(query, queryTokens)) {
      return;
    }

    String tableName = Utils.InsertCommand.getTableNameFromTokens(queryTokens);

    if (!Utils.InsertCommand.checkForEmptyTableName(tableName)) {
      return;
    }

    try {
      tableName = Utils.InsertCommand.parseTableNameFromQuery(tableName);

      List<ColumnInfo> attributesListWithProps = new ArrayList<>();

      List<String> attributesTokens = Utils.CreateTable.getParsedValuesTokens(query);

      short ordinalPosition = 1;
      String primaryKeyColumn = "";

      for (String attribute_ : attributesTokens) {
        List<String> prop = Utils.CreateTable.getTrimmedSpaceSeparatedTokens(attribute_);

        ColumnInfo columnConstraints = new ColumnInfo(tableName, prop.get(0),
            DataType.get(prop.get(1).toUpperCase()), true);

        for (int i = 0; i < prop.size(); i++) {
          if (prop.get(i).equals(Constants.NULL_STRING)) {
            columnConstraints.isNullable = true;
          }

          if (prop.get(i).contains(Constants.NOT_STRING) && (prop.get(i + 1).contains(Constants.NULL_STRING))) {
            columnConstraints.isNullable = false;
            i++;
          }

          if ((prop.get(i).equals(Constants.UNIQUE_STRING))) {
            columnConstraints.isUnique = true;
          } else if (prop.get(i).contains(Constants.PRIMARY_STRING) && prop.get(i + 1).contains("key")) {
            columnConstraints.isPrimaryKey = true;
            columnConstraints.isUnique = true;
            columnConstraints.isNullable = false;
            primaryKeyColumn = columnConstraints.columnName;
            i++;
          }
        }

        columnConstraints.ordinalPosition = ordinalPosition++;

        attributesListWithProps.add(columnConstraints);
      }

      RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
          Utils.getTBLFilePath(DavisBaseBinaryFile.davisbaseTables), Constants.READ_WRITE_MODE);

      TableMetaData davisbaseTableMetaData = new TableMetaData(DavisBaseBinaryFile.davisbaseTables);

      int pageNum = BPlusOneTree.getPageNumForInsertion(davisbaseTablesCatalog, davisbaseTableMetaData.rootPageNum);

      Page page = new Page(davisbaseTablesCatalog, pageNum);

      int rowNum = page.addTableRow(DavisBaseBinaryFile.davisbaseTables,
          Arrays.asList(new Attribute(DataType.TEXT, tableName),
              new Attribute(DataType.INT, "0"), new Attribute(DataType.SMALLINT, "0"),
              new Attribute(DataType.SMALLINT, "0")));

      Utils.closeIOFile(davisbaseTablesCatalog);

      if (rowNum == -1) {
        Utils.log(ParseCreateTable.DUPLICATE_TABLE_ERROR);
        return;
      }

      RandomAccessFile tableFile = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_WRITE_MODE);

      Page.addNewPage(tableFile, Page.PageType.LEAF, -1, -1);

      Utils.closeIOFile(tableFile);

      RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
          Utils.getTBLFilePath(DavisBaseBinaryFile.davisbaseColumns), Constants.READ_WRITE_MODE);

      TableMetaData davisbaseColumnsMetaData = new TableMetaData(DavisBaseBinaryFile.davisbaseColumns);

      pageNum = BPlusOneTree.getPageNumForInsertion(davisbaseColumnsCatalog, davisbaseColumnsMetaData.rootPageNum);

      Page page_ = new Page(davisbaseColumnsCatalog, pageNum);

      for (ColumnInfo attribute : attributesListWithProps) {
        page_.addNewColumn(attribute);
      }

      Utils.closeIOFile(davisbaseColumnsCatalog);

      Utils.log(tableName + ParseCreateTable.TABLE_CREATION_SUCCESS);

      Utils.CreateTable.createPrimaryKeyIndex(primaryKeyColumn, tableName);

    } catch (Exception e) {
      Utils.log(ParseCreateTable.TABLE_CREATION_ERROR);
      Utils.log(e.getMessage());

      String deleteTablesQueryStr = Utils.DropCommand.generateDropDeleteQuery(tableName,
          DavisBaseBinaryFile.davisbaseTables);

      String deleteColumnsQueryStr = Utils.DropCommand.generateDropDeleteQuery(tableName,
          DavisBaseBinaryFile.davisbaseColumns);

      parseDelete(deleteTablesQueryStr);
      parseDelete(deleteColumnsQueryStr);
    }
  }

  // method to parse DELETE user query command
  public static void parseDelete(String query) {
    // Query: DELETE FROM TABLE <table_name> WHERE <condition>;

    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    String tableName = "";

    try {
      if (!Utils.DeleteCommand.checkDeleteCommandSyntax(query, queryTokens)) {
        return;
      }

      tableName = Utils.DeleteCommand.getTableNameFromTokens(queryTokens);

      TableMetaData metaData = new TableMetaData(tableName);

      Condition condition;
      try {
        condition = Utils.QueryCondition.fetchAndParseQueryCondition(metaData, query);
      } catch (Exception e) {
        Utils.log(e + "");
        return;
      }

      RandomAccessFile tableFile = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_WRITE_MODE);

      BPlusOneTree tree = new BPlusOneTree(tableFile, metaData.rootPageNum, metaData.tableName);

      List<TableRecord> deletedRecords = Utils.DeleteCommand.getDeletedRecords(tree, tableFile, tableName, condition);

      if (condition == null) {
      } else {
        Utils.DeleteCommand.deleteExistingIndex(metaData, tableName, deletedRecords);
      }

      Utils.log(ParseDelete.RECORDS_DELETE_SUCCESS + tableName);

      Utils.closeIOFile(tableFile);
    } catch (Exception e) {
      Utils.log(ParseDelete.RECORDS_DELETE_ERROR + tableName);
      Utils.log(e.getMessage());
    }
  }

  // method to parse input user query command
  public static void parseUserQuery(String query) {
    List<String> queryTokens = Utils.getSpaceSeparatedTokens(query);

    switch (queryTokens.get(0)) {
      case Constants.SHOW_STRING:
        if (queryTokens.get(1).equals(Constants.TABLES_STRING)) {
          parseUserQuery(ParseUserQuery.SELECT_DAVISBASE_TABLES);
        } else if (queryTokens.get(1).equals("rowid")) {
          DavisBaseBinaryFile.showRowId = true;
          Utils.log(ParseUserQuery.ROWID_SHOW_SUCCESS);
        } else {
          Utils.log(ParseUserQuery.COMMAND_RECHECK_ERROR + query + "\"");
        }
        Utils.log("");
        break;

      case Constants.SELECT_STRING:
        parseQuery(query);
        Utils.log("");
        break;

      case Constants.DROP_STRING:
        dropTable(query);
        Utils.log("");
        break;

      case Constants.CREATE_STRING:
        if (queryTokens.get(1).equals("table"))
          parseCreateTable(query);
        else if (queryTokens.get(1).equals("index"))
          parseCreateIndex(query);
        Utils.log("");
        break;

      case Constants.UPDATE_STRING:
        parseUpdate(query);
        Utils.log("");
        break;

      case Constants.INSERT_STRING:
        parseInsert(query);
        Utils.log("");
        break;

      case Constants.DELETE_STRING:
        parseDelete(query);
        Utils.log("");
        break;

      case Constants.HELP_STRING:
        Utils.displayHelpScreen();
        Utils.log("");
        break;

      case Constants.VERSION_STRING:
        Utils.displayVersion();
        Utils.log("");
        break;

      case Constants.EXIT_STRING:
      case Constants.QUIT_STRING:
        QueryInput.exit = true;
        break;

      default:
        Utils.log(ParseUserQuery.COMMAND_RECHECK_ERROR + query + "\"");
        Utils.log("");
    }
  }

}
