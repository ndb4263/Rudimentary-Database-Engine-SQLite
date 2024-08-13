package parsing;

import java.io.File;
import java.util.Scanner;

import storage.DavisBaseBinaryFile;
import utils.Utils;
import utils.Constants;

public class QueryInput {

  static boolean exit = false;
  static Scanner scanner = new Scanner(System.in).useDelimiter(";");

  public static void init(String[] args) {
    Utils.splashScreen();

    File filePath = new File("data");

    if (!new File(filePath, DavisBaseBinaryFile.davisbaseTables + ".tbl").exists()
        || !new File(filePath, DavisBaseBinaryFile.davisbaseColumns + ".tbl").exists()) {
      DavisBaseBinaryFile.initializeDataStore();
    } else {
      DavisBaseBinaryFile.dataStoreInitialized = true;
    }

    String userInputQuery;

    while (!exit) {
      Utils.logger(Constants.PROMPT);
      userInputQuery = scanner.next();
      userInputQuery = Utils.replaceNewLinesAndReturn(userInputQuery);
      Query.parseUserQuery(userInputQuery);
    }

    Utils.log("");
  }

}
