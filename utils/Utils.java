package utils;

import java.util.*;
import java.io.*;

import parsing.Query;
import storage.Attribute;
import storage.BPlusOneTree;
import storage.BTree;
import utils.Constants.ParseDrop;
import utils.Constants.ParseInsert;
import utils.Constants.ParseUpdate;
import utils.Constants.SplashScreen;
import storage.DataType;
import storage.DavisBaseBinaryFile;
import storage.Page;
import utils.Constants.CreateIndex;
import utils.Constants.ParseCreateTable;
import utils.Constants.ParseDelete;

public class Utils {

  public static void splashScreen() {
    Utils.log(line(SplashScreen.DIVIDER_STR, 80));
    Utils.log(SplashScreen.LINE_STR_1);
    Utils.log(SplashScreen.LINE_STR_2 + Constants.VERSION);
    Utils.log(SplashScreen.LINE_STR_3);
    Utils.log(line(SplashScreen.DIVIDER_STR, 80));
  }

  public static void displayHelpScreen() {
    Utils.log("SUPPORTED COMMANDS\n");
    Utils.log("All Commands below are CASE INSENSITIVE\n");

    Utils.log("SHOW TABLES;");
    Utils.log("\tDisplay all table names.\n");

    Utils.log("CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);");
    Utils.log("\tCreates a table with the specified columns.\n");

    Utils.log("CREATE INDEX ON <table_name> (<column_name>);");
    Utils.log("\tCreates an Index on a Column in the table. \n");

    Utils.log("INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
    Utils.log("\tInserts a new record into the table with the given values for the given columns.\n");

    Utils.log("DELETE FROM TABLE <table_name> [WHERE <condition>];");
    Utils.log("\tDelete one or more table records whose optional <condition>");
    Utils.log("\tis <column_name> = <value>.\n");

    Utils.log("DROP TABLE <table_name>;");
    Utils.log("\tRemove table data (all records) and its schema.\n");

    Utils.log("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
    Utils.log("\tModify records data whose optional <condition>");
    Utils.log("\tis <column_name> = <value>.\n");

    Utils.log("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
    Utils.log("\tDisplay table records whose optional <condition>");
    Utils.log("\tis <column_name> = <value>.\n");

    Utils.log("VERSION;");
    Utils.log("\tDisplay the program version.\n");

    Utils.log("HELP;");
    Utils.log("\tDisplay this help information.\n");

    Utils.log("EXIT;");
    Utils.log("\tExit the program.\n");

    Utils.log(Utils.line("*", 80));
  }

  public static String getTBLFilePath(String tableName) {
    return "data/" + tableName + ".tbl";
  }

  public static String getNDXFilePath(String tableName, String columnName) {
    return "data/" + tableName + "_" + columnName + ".ndx";
  }

  public static String line(String s, int num) {
    return String.valueOf(s).repeat(Math.max(0, num));
  }

  public static String[] getSpaceSeparatedArray(String str) {
    return str.split(" ");
  }

  public static String[] getCommaSeparatedArray(String str) {
    return str.split(",");
  }

  public static String removeLeadingTrailingWhitespaces(String str) {
    return str.trim();
  }

  public static String removeQuotesFromString(String str) {
    return str.replace("\"", "").replace("'", "");
  }

  public static String replaceNewLinesAndReturn(String str) {
    return Utils.removeLeadingTrailingWhitespaces(str.replace("\n", " ").replace("\r", "")).toLowerCase();
  }

  public static List<String> getSpaceSeparatedTokens(String str) {
    return new ArrayList<>(Arrays.asList(str.split(" ")));
  }

  public static int getColumnOrdinalPosition(TableMetaData metaData, String col) {
    return metaData.colNames.indexOf(col);
  }

  public static void closeIOFile(RandomAccessFile file) throws IOException {
    file.close();
  }

  public static void displayVersion() {
    Utils.log(SplashScreen.LINE_STR_2 + Constants.VERSION);
  }

  public static void log(String str) {
    System.out.println(str);
  }

  public static void logger(String str) {
    System.out.print(str);
  }

  public static class CreateIndexCommand {
    public static boolean checkParseCreateIndexQuerySyntax(String queryStr, List<String> space_separated_tokens) {
      boolean is_valid = true;
      if (!space_separated_tokens.get(2).equals("on") || !queryStr.contains("(")
          || !queryStr.contains(")") && space_separated_tokens.size() < 4) {
        is_valid = false;
        Utils.log(CreateIndex.SYNTAX_ERROR);
      }
      return is_valid;
    }

    public static String getTableNameFromQuery(String q) {
      String tableName = Utils.removeLeadingTrailingWhitespaces(q.substring(q.indexOf("on") + 3, q.indexOf("(")));
      return tableName;
    }

    public static boolean isTableValid(RandomAccessFile tableFile, TableMetaData metaData) throws IOException {
      boolean is_valid = true;
      if (!metaData.doesTableExists) {
        is_valid = false;
        Utils.log(CreateIndex.TABLE_NAME_INVALID_ERROR);
        Utils.closeIOFile(tableFile);
      }
      return is_valid;
    }

    public static boolean isColumnNameValid(RandomAccessFile tableFile, int columnOrdinal) throws IOException {
      boolean is_valid = true;
      if (columnOrdinal < 0) {
        is_valid = false;
        Utils.log(CreateIndex.COLUMN_NAME_INVALID_ERROR);
        Utils.closeIOFile(tableFile);
      }
      return is_valid;
    }

    public static String getColumnNameFromQuery(String q) {
      String columnName = Utils.removeLeadingTrailingWhitespaces(q.substring(q.indexOf("(") + 1, q.indexOf(")")));
      return columnName;
    }

    public static boolean checkIfIndexExists(String tableName, String columnName) {
      boolean exists = false;
      if (new File(getNDXFilePath(tableName, columnName)).exists()) {
        exists = true;
        Utils.log(CreateIndex.ALREADY_EXISTS_ERROR);
      }
      return exists;
    }
  }

  public static class DropCommand {
    public static String generateDropDeleteQuery(String tableInput, String tableFile) {
      return "delete from table " + tableFile + " where table_name = '" + tableInput + "' ";
    }

    public static void deleteTableFile(String tableName) {
      File tableFile = new File(getTBLFilePath(tableName));
      if (tableFile.delete())
        Utils.log("Table " + tableName + " deleted");
      else
        Utils.log(ParseDrop.TABLE_ERROR);
    }

    public static boolean checkDropCommandSyntax(String[] q_tokens) {
      boolean is_valid = true;
      if (!(q_tokens[0].trim().equalsIgnoreCase(Constants.DROP_STRING_UPPERCASE)
          && q_tokens[1].trim().equalsIgnoreCase(Constants.TABLE_STRING_UPPERCASE))) {
        is_valid = false;
        Utils.log(ParseDrop.SYNTAX_ERROR);
      }
      return is_valid;
    }

    public static void deleteIndexes(String tableName) {
      File file = new File("data/");
      File matchingFiles[] = file.listFiles((dir, name) -> name.startsWith(tableName) && name.endsWith("ndx"));

      boolean iFlag = false;
      assert matchingFiles != null;

      for (File file_ : matchingFiles) {
        if (file_.delete()) {
          iFlag = true;
          Utils.log(ParseDrop.INDEX_DROP_SUCCESS);
        }
      }

      if (iFlag == true)
        Utils.log("DROP " + tableName);
      else
        Utils.log(ParseDrop.INDEX_ERROR);
    }
  }

  public static class ParseQuery {
    public static List<List<String>> parseTableAndColNames(List<String> q_tokens) {
      List<String> columnNames = new ArrayList<>();
      String tableName = "";

      for (int i_val = 1; i_val < q_tokens.size(); i_val++) {
        if (q_tokens.get(i_val).equals("from")) {
          tableName = q_tokens.get(i_val + 1);
          break;
        }

        if (!q_tokens.get(i_val).equals("*") && !q_tokens.get(i_val).equals(",")) {
          if (q_tokens.get(i_val).contains(",")) {
            ArrayList<String> colList = new ArrayList<>(Arrays.asList(q_tokens.get(i_val).split(",")));
            for (String col : colList)
              columnNames.add(col.trim());
          } else
            columnNames.add(q_tokens.get(i_val));
        }
      }

      List<String> tableNameRes = new ArrayList<>();
      tableNameRes.add(tableName);

      List<List<String>> result = new ArrayList<>();
      result.add(tableNameRes);
      result.add(columnNames);
      return result;
    }

    public static boolean isTableValid(TableMetaData data, String name) {
      boolean is_valid = true;
      if (!data.doesTableExists) {
        is_valid = false;
        Utils.log("Table " + name + " does not exist.");
      }
      return is_valid;
    }

    public static boolean areAllColumnsRequested(List<String> columnNames) {
      boolean result = columnNames.size() == 0;
      return result;
    }

    public static void fetchRecordsBasedOnCondition(DavisBaseBinaryFile tableBinaryFile, TableMetaData tableMetaData,
        Condition condition, List<String> columnNames) throws IOException {
      tableBinaryFile.selectRecords(tableMetaData, columnNames, condition);
    }
  }

  public static class UpdateCommand {
    public static boolean checkUpdateQuerySyntax(List<String> q_tokens) {
      boolean is_valid = true;
      if (!q_tokens.get(2).equals("set") || !q_tokens.contains("=")) {
        is_valid = false;
        Utils.log(ParseUpdate.SYNTAX_ERROR);
        Utils.log(ParseUpdate.EXPECTED_SYNTAX);
      }
      return is_valid;
    }

    public static String getTableNameFromTokens(List<String> q_tokens) {
      return q_tokens.get(1);
    }

    public static boolean areColumnNamesValid(List<String> columnsToUpdate, TableMetaData metaData) {
      boolean is_valid = true;
      if (!metaData.columnExists(columnsToUpdate)) {
        is_valid = false;
        Utils.log(ParseUpdate.COLUMN_NAME_ERROR);
      }
      return is_valid;
    }

    public static boolean isTableNameValid(TableMetaData metaData) {
      boolean is_valid = true;
      if (!metaData.doesTableExists) {
        is_valid = false;
        Utils.log(ParseUpdate.TABLE_NAME_ERROR);
      }
      return is_valid;
    }

    public static int getRecordsCountTobeUpdated(List<String> columnsToUpdate, List<String> valueToUpdate,
        Condition condition, DavisBaseBinaryFile file, TableMetaData metaData) throws IOException {
      return file.updateRecords(metaData, condition, columnsToUpdate, valueToUpdate);
    }

    public static String getTargetValAndCol(String q) {
      return q.split("set")[1].split("where")[0];
    }

    public static List<List<String>> populateTargetColumnsAndValues(String q) {
      List<String> columnsToUpdate = new ArrayList<>();
      List<String> valueToUpdate = new ArrayList<>();

      String updateColInfoString = getTargetValAndCol(q);
      String column_newValueSet[] = Utils.getCommaSeparatedArray(updateColInfoString);

      for (String item_col : column_newValueSet) {
        String splittedStr[] = item_col.split("=");
        columnsToUpdate.add(Utils.removeLeadingTrailingWhitespaces(splittedStr[0]));
        valueToUpdate.add(Utils.removeQuotesFromString(Utils.removeLeadingTrailingWhitespaces(splittedStr[1])));
      }

      List<List<String>> res = new ArrayList<>();
      res.add(columnsToUpdate);
      res.add(valueToUpdate);
      return res;
    }
  }

  public static class InsertCommand {
    public static boolean checkForEmptyTableName(String name) {
      boolean is_valid = true;
      if (Utils.removeLeadingTrailingWhitespaces(name).length() == 0) {
        is_valid = false;
        Utils.log(ParseInsert.TABLE_NAME_ERROR);
      }
      return is_valid;
    }

    public static String getTableNameFromTokens(List<String> q_tokens) {
      return q_tokens.get(2);
    }

    public static boolean checkInsertQuerySyntax(String q, List<String> q_tokens) {
      boolean is_valid = true;
      if (!q_tokens.get(1).equals("into") || !q.contains(") values")) {
        is_valid = false;
        Utils.log(ParseInsert.SYNTAX_ERROR);
        Utils.log(ParseInsert.EXPECTED_SYNTAX);
      }
      return is_valid;
    }

    public static boolean isTableValid(TableMetaData metaData) {
      boolean is_valid = true;
      if (!metaData.doesTableExists) {
        is_valid = false;
        Utils.log(ParseInsert.TABLE_NAME_ERROR_2);
      }
      return is_valid;
    }

    public static List<String> getColumnTokens(String q) {
      return new ArrayList<>(
          Arrays.asList(Utils.getCommaSeparatedArray(q.substring(q.indexOf("(") + 1, q.indexOf(") values")))));
    }

    public static String parseTableNameFromQuery(String q) {
      String tname = q;
      if (q.contains("(")) {
        tname = q.substring(0, q.indexOf("("));
      }
      return tname;
    }

    public static boolean areColumnsValid(List<String> columnTokens, TableMetaData metaData) {
      boolean is_valid = true;
      for (String colToken : columnTokens) {
        if (!metaData.colNames.contains(Utils.removeLeadingTrailingWhitespaces(colToken))) {
          is_valid = false;
          Utils.log("Invalid column : " + Utils.removeLeadingTrailingWhitespaces(colToken));
          break;
        }
      }
      return is_valid;
    }

    public static List<String> getParsedValuesTokens(String q) {
      return new ArrayList<>(Arrays.asList(Utils.getCommaSeparatedArray(q.substring(q.indexOf("(") + 1))));
    }

    public static String getParsedValues(String q) {
      return q.substring(q.indexOf("values") + 6, q.length() - 1);
    }

    public static void removeExcessTokens(int index, List<String> columnTokens, List<String> valueTokens) {
      if (columnTokens.size() > index) {
        columnTokens.remove(index);
        valueTokens.remove(index);
      }
    }

    public static void updateColumnIndexes(TableMetaData metaData, String tname, List<Attribute> attributeToInsert,
        int rowNum) throws Exception {
      if (rowNum != -1) {
        for (int i_val = 0; i_val < metaData.colNameAttributes.size(); i_val++) {
          ColumnInfo col = metaData.colNameAttributes.get(i_val);
          if (col.hasIndex) {
            RandomAccessFile indexFile = new RandomAccessFile(Utils.getNDXFilePath(tname, col.columnName),
                Constants.READ_WRITE_MODE);
            BTree bTree = new BTree(indexFile);
            bTree.insert(attributeToInsert.get(i_val), rowNum);
          }
        }
      }
    }

    public static boolean handleIfColNotPresentInQuery(boolean columnProvided, ColumnInfo colInfo,
        List<Attribute> attributeToInsert) throws Exception {
      boolean is_valid = true;
      if (!columnProvided) {
        if (colInfo.isNullable)
          attributeToInsert.add(new Attribute(DataType.NULL, "NULL"));
        else {
          is_valid = false;
          Utils.log("Cannot Insert NULL into " + colInfo.columnName);
        }
      }
      return is_valid;
    }

    public static void parseAttributesAndValues() {
      //
    }
  }

  public static class CreateTable {
    public static boolean checkCreateCommandSyntax(String q, List<String> q_tokens) {
      boolean is_valid = true;
      if (!q_tokens.get(1).equals(Constants.TABLE_STRING)) {
        is_valid = false;
        Utils.log(ParseCreateTable.SYNTAX_ERROR);
      }
      return is_valid;
    }

    public static List<String> getParsedValuesTokens(String q) {
      return new ArrayList<>(
          Arrays.asList(Utils.getCommaSeparatedArray(q.substring(q.indexOf("(") + 1, q.length() - 1))));
    }

    public static void createPrimaryKeyIndex(String primaryKeyColumn, String tableName) {
      if (primaryKeyColumn.length() > 0) {
        Query.parseCreateIndex("create index on " + tableName + "(" + primaryKeyColumn + ")");
      }
    }

    public static List<String> getTrimmedSpaceSeparatedTokens(String q) {
      return new ArrayList<>(Arrays.asList(Utils.removeLeadingTrailingWhitespaces(q).split(" ")));
    }
  }

  public static class DeleteCommand {
    public static void deleteExistingIndex(TableMetaData metaData, String tableName, List<TableRecord> deleted_records)
        throws Exception {
      for (int i_val = 0; i_val < metaData.colNameAttributes.size(); i_val++) {
        if (metaData.colNameAttributes.get(i_val).hasIndex) {
          RandomAccessFile indexFile = new RandomAccessFile(
              Utils.getNDXFilePath(tableName, metaData.colNameAttributes.get(i_val).columnName),
              Constants.READ_WRITE_MODE);
          BTree bTree = new BTree(indexFile);
          for (TableRecord r : deleted_records) {
            bTree.delete(r.getAttributes().get(i_val), r.rowId);
          }
        }
      }
    }

    public static boolean checkDeleteCommandSyntax(String q, List<String> q_tokens) {
      boolean is_valid = true;
      if (!q_tokens.get(1).equals("from") || !q_tokens.get(2).equals(Constants.TABLE_STRING)) {
        is_valid = false;
        Utils.log(ParseDelete.SYNTAX_ERROR);
      }
      return is_valid;
    }

    public static List<TableRecord> getDeletedRecords(BPlusOneTree tree, RandomAccessFile tableFile, String tableName,
        Condition condition) throws Exception {
      List<TableRecord> deleted_records = new ArrayList<>();

      for (int pageNum : tree.getAllLeaves(condition)) {
        short deleteCountPerPage = 0;
        Page page = new Page(tableFile, pageNum);

        for (TableRecord r : page.getPageRecords()) {
          if (condition != null) {
            if (!condition.checkCondition(r.getAttributes().get(condition.columnOrdinal).fieldValue)) {
              continue;
            }
          }
          deleted_records.add(r);
          page.DeleteTableRecord(tableName, Integer.valueOf(r.pageHeaderIndex - deleteCountPerPage).shortValue());
          deleteCountPerPage++;
        }
      }

      return deleted_records;
    }

    public static String getTableNameFromTokens(List<String> q_tokens) {
      return q_tokens.get(3);
    }
  }

  public static class QueryCondition {
    public static Condition fetchAndParseQueryCondition(TableMetaData tableMetaData, String q) throws Exception {
      if (q.contains("where")) {
        Condition condition = new Condition(DataType.TEXT);
        String whereClause = q.substring(q.indexOf("where") + 6);
        List<String> where_clause_tokens = new ArrayList<>(Arrays.asList(whereClause.split(" ")));

        if (where_clause_tokens.get(0).equalsIgnoreCase(Constants.NOT_STRING))
          condition.setNegation(true);

        for (int i_val = 0; i_val < Condition.supportedOperators.length; i_val++) {
          if (whereClause.contains(Condition.supportedOperators[i_val])) {
            where_clause_tokens = new ArrayList<>(
                Arrays.asList(whereClause.split(Condition.supportedOperators[i_val])));
            {
              condition.setOperator(Condition.supportedOperators[i_val]);
              condition.setConditionValue(where_clause_tokens.get(1).trim());
              condition.setColumName(where_clause_tokens.get(0).trim());
              break;
            }
          }
        }

        if (tableMetaData.doesTableExists
            && tableMetaData.columnExists(new ArrayList<>(List.of(condition.columnName)))) {
          condition.columnOrdinal = tableMetaData.colNames.indexOf(condition.columnName);
          condition.dataType = tableMetaData.colNameAttributes.get(condition.columnOrdinal).dataType;
        } else {
          throw new Exception(
              "ERROR :: Invalid table or column: " + tableMetaData.tableName + " . " + condition.columnName);
        }

        return condition;
      } else {
        return null;
      }
    }
  }
}
