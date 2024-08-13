package utils;

public class Constants {

  public static String PROMPT = "DavisBase-Bohr> ";
  public static String VERSION = "v1.0";
  // public static String COPYRIGHT = "* HexDump (c)2022 Chris Irwin Davis";
  public static int PAGE_SIZE_POWER = 9;

  public enum OperatorType {
    LESSTHAN,
    EQUALTO,
    GREATERTHAN,
    LESSTHANOREQUAL,
    GREATERTHANOREQUAL,
    NOTEQUAL,
    INVALID
  }

  public static final String READ_WRITE_MODE = "rw";
  public static final String READ_MODE = "r";

  public static final String EQUALS_OP = "=";

  public static final String NULL_STRING = "null";
  public static final String NOT_STRING = "not";
  public static final String UNIQUE_STRING = "unique";
  public static final String PRIMARY_STRING = "primary";
  public static final String TABLES_STRING = "tables";
  public static final String TABLE_STRING = "table";

  public static final String SHOW_STRING = "show";
  public static final String SELECT_STRING = "select";
  public static final String DROP_STRING = "drop";
  public static final String CREATE_STRING = "create";
  public static final String UPDATE_STRING = "update";
  public static final String INSERT_STRING = "insert";
  public static final String DELETE_STRING = "delete";
  public static final String HELP_STRING = "help";
  public static final String VERSION_STRING = "version";
  public static final String EXIT_STRING = "exit";
  public static final String QUIT_STRING = "quit";

  public static final String DROP_STRING_UPPERCASE = "DROP";
  public static final String TABLE_STRING_UPPERCASE = "TABLE";

  public class SplashScreen {

    public static final String DIVIDER_STR = "-";
    public static final String LINE_STR_1 = "Welcome to DavisBase (by Team 'BOHR')";
    public static final String LINE_STR_2 = "DavisBase Version ";
    public static final String LINE_STR_3 = "\nType \"HELP;\" to display supported commands.";

  }

  public class CreateIndex {

    public static String INDEX_CREATED = "Index successfully created on column: ";

    public static String INDEX_CREATION_ERROR = "ERROR :: Index cannot be created";

    public static String SYNTAX_ERROR = "ERROR :: Incorrect syntax";

    public static String ALREADY_EXISTS_ERROR = "ERROR :: Index already exists";

    public static String TABLE_NAME_INVALID_ERROR = "ERROR :: Invalid Table name";
    public static String COLUMN_NAME_INVALID_ERROR = "ERROR :: Invalid column name";

  }

  public class ParseQuery {

    public static String DATA_ERROR = "ERROR :: Data cannot be retrieved";

  }

  public class ParseUpdate {

    public static String TABLE_UPDATE_ERROR = "ERROR :: Cannot update the table file";
    public static String SYNTAX_ERROR = "ERROR :: Check query syntax";
    public static String TABLE_NAME_ERROR = "ERROR :: Invalid table name";
    public static String COLUMN_NAME_ERROR = "ERROR :: Invalid column name(s)";
    public static String EXPECTED_SYNTAX = "Expected Syntax: UPDATE [table_name] SET [Column_name] = val1 where [column_name] = val2; ";

  }

  public class ParseInsert {

    public static String NULL_INSERTION_ERROR = "ERROR :: Cannot insert 'NULL' into ";

    public static String DATA_ERROR_1 = "ERROR :: Data format INVALID for ";
    public static String DATA_ERROR_2 = " values: ";

    public static String INSERTION_SUCCESS = "Record inserted successfully";

    public static String INSERTION_ERROR = "ERROR :: Record cannot be inserted";

    public static String SYNTAX_ERROR = "ERROR :: Check query syntax";

    public static String EXPECTED_SYNTAX = "Expected Syntax: INSERT INTO table_name ( columns ) VALUES ( values ); ";

    public static String TABLE_NAME_ERROR = "ERROR :: Empty table name";

    public static String TABLE_NAME_ERROR_2 = "ERROR :: Table does not exist";

  }

  public class ParseCreateTable {

    public static String DUPLICATE_TABLE_ERROR = "ERROR :: Duplicate table name exists";

    public static String TABLE_CREATION_SUCCESS = " table successfully created";

    public static String TABLE_CREATION_ERROR = "ERROR :: Table cannot be created";

    public static String SYNTAX_ERROR = "ERROR :: Check query syntax";
  }

  public class ParseDrop {

    public static String TABLE_ERROR = "ERROR :: Table does not exist";
    public static String INDEX_ERROR = "ERROR :: Index does not exist";
    public static String SYNTAX_ERROR = "ERROR :: Check query syntax";
    public static String INDEX_DROP_SUCCESS = "Index successfully deleted";

  }

  public class ParseDelete {

    public static String RECORDS_DELETE_SUCCESS = "Record(s) deleted from ";

    public static String RECORDS_DELETE_ERROR = "ERROR :: Rows cannot be deleted in table ";

    public static String SYNTAX_ERROR = "ERROR :: Check query syntax";
  }

  public class ParseUserQuery {

    public static String SELECT_DAVISBASE_TABLES = "select * from davisbase_tables";

    public static String ROWID_SHOW_SUCCESS = "Table Select will now include RowId";

    public static String COMMAND_RECHECK_ERROR = "ERROR :: Re-check the command \"";
  }

  public class RecordOperations {

    public static String FETCH_ROOT_PAGE_ERROR = "ERROR :: fetching the root page No.";

    public static String INIT_DATA_STORE_ERROR = "ERROR :: Unable to create data container directory";

    public static String CREATE_TABLES_ERROR = "ERROR :: Unable to create the database_tables file";

    public static String CREATE_COLUMNS_ERROR = "ERROR :: Unable to create the database_columns file";

    public static String RECORD_DELETE_ERROR = "ERROR :: Unable to delete record ";
  }

  public class PageOperations {

    public static String PAGE_READ_ERROR = "ERROR :: Unable to read the page ";

    public static String PAGE_FETCH_ERROR = "ERROR :: Unable to fetch the page type ";

    public static String PAGE_ADD_ERROR = "ERROR :: Unable to add new page ";

    public static String COLUMN_ADD_ERROR = "ERROR :: Unable to add column ";

    public static String TABLE_OVERFLOW_ERROR = "ERROR :: Handle table overflow ";

    public static String INDEX_FILE_INSERT_ERROR = "ERROR :: Unable to insert index file while splitting index files";

    public static String HEADER_OFFSET_REFRESH_ERROR = "ERROR :: Unable to refresh header offset ";

    public static String RECORD_FILL_ERROR = "ERROR :: Unable to fill records from page ";

    public static String PAGE_OVERFLOW_ERROR = "ERROR :: Maximum rows reached for index file, page size to be increased to avoid page overflow";

    public static String ATTRIBUTE_DELETE_ERROR = "ERROR :: Unable to delete the attribute from index page ";

    public static String ATTRIBUTE_INSERT_ERROR = "ERROR :: Unable to insert the attribute into index page ";
  }

  public class TableMetaData {

    public static String TABLE_ERROR = "ERROR :: Table does not exist";
    public static String COLUMN_DATA_ERROR = "ERROR :: Cannot get column data for ";
    public static String META_DATA_ERROR = "ERROR :: Cannot get meta data for ";
    public static String INSERTION_ERROR_1 = "ERROR :: Insertion failed. Column ";
    public static String INSERTION_ERROR_2 = " should be unique";

  }

  public class AttributeStrings {

    public static String DATE_FORMAT_STR = "yyyy-MM-dd";
    public static String DATETIME_FORMAT_STR = "yyyy-MM-dd_HH:mm:ss";

    public static String FORMAT_ERROR = "ERROR :: Issue in formatting";
    public static String CONVERT_ERROR_1 = "ERROR :: Cannot convert ";
    public static String CONVERT_ERROR_2 = " to ";
  }

  public class AttributeOperations {
    public static String FORMATTING_ERROR = "ERROR :: Unable to format ";

    public static String CONVERSION_ERROR = "ERROR :: Unable to convert ";
  }
}
