package utils;

import java.util.*;

import storage.DataType;
import utils.Constants.OperatorType;

public class Condition {
  public DataType dataType;
  public String columnName;
  public boolean negation;
  public int columnOrdinal;
  private OperatorType operator;
  public String comparisonValue;

  public Condition(DataType dataType) {
    this.dataType = dataType;
  }

  public static String[] supportedOperators = {
      "<=", ">=", "<>", ">", "<", "="
  };

  public static OperatorType getOperatorType(String str_operator) {
    switch (str_operator) {
      case "<>":
        return OperatorType.NOTEQUAL;

      case "=":
        return OperatorType.EQUALTO;

      case ">":
        return OperatorType.GREATERTHAN;

      case ">=":
        return OperatorType.GREATERTHANOREQUAL;

      case "<":
        return OperatorType.LESSTHAN;

      case "<=":
        return OperatorType.LESSTHANOREQUAL;

      default:
        Utils.log("! Invalid operator \"" + str_operator + "\"");
        return OperatorType.INVALID;
    }
  }

  public static int compare(String val_1, String val_2, DataType dataType) {
    if (dataType == DataType.TEXT) {
      val_1 = val_1.toLowerCase();
      return val_1.compareTo(val_2);
    }

    else if (dataType == DataType.NULL) {
      if (Objects.equals(val_1, val_2))
        return 0;

      else if (val_1.equalsIgnoreCase("null"))
        return 1;

      else
        return -1;
    }

    else {
      return Long.valueOf(Long.parseLong(val_1) - Long.parseLong(val_2)).intValue();
    }
  }

  private boolean doOperationOnDifference(OperatorType op, int diff) {
    switch (op) {
      case EQUALTO:
        return diff == 0;

      case NOTEQUAL:
        return diff != 0;

      case LESSTHAN:
        return diff < 0;

      case LESSTHANOREQUAL:
        return diff <= 0;

      case GREATERTHANOREQUAL:
        return diff >= 0;

      case GREATERTHAN:
        return diff > 0;

      default:
        return false;
    }
  }

  private boolean doStringCompare(String currentValue, OperatorType op) {
    return doOperationOnDifference(op, currentValue.toLowerCase().compareTo(comparisonValue));
  }

  public boolean checkCondition(String currentValue) {
    OperatorType op = getOperation();

    if (currentValue.equalsIgnoreCase("null") || comparisonValue.equalsIgnoreCase("null")) {
      return doOperationOnDifference(op, compare(currentValue, comparisonValue, DataType.NULL));
    }

    if (dataType == DataType.TEXT || dataType == DataType.NULL) {
      return doStringCompare(currentValue, op);
    } else {
      switch (op) {
        case EQUALTO:
          return Long.parseLong(currentValue) == Long.parseLong(comparisonValue);

        case NOTEQUAL:
          return Long.parseLong(currentValue) != Long.parseLong(comparisonValue);

        case LESSTHAN:
          return Long.parseLong(currentValue) < Long.parseLong(comparisonValue);

        case LESSTHANOREQUAL:
          return Long.parseLong(currentValue) <= Long.parseLong(comparisonValue);

        case GREATERTHAN:
          return Long.parseLong(currentValue) > Long.parseLong(comparisonValue);

        case GREATERTHANOREQUAL:
          return Long.parseLong(currentValue) >= Long.parseLong(comparisonValue);

        default:
          return false;
      }
    }
  }

  public void setConditionValue(String conditionValue) {
    this.comparisonValue = conditionValue;
    this.comparisonValue = comparisonValue.replace("'", "");
    this.comparisonValue = comparisonValue.replace("\"", "");
  }

  public void setOperator(String operator) {
    this.operator = getOperatorType(operator);
  }

  public OperatorType getOperation() {
    if (!negation) {
      return this.operator;
    } else {
      return negateOperator();
    }
  }

  public void setColumName(String columnName) {
    this.columnName = columnName;
  }

  public void setNegation(boolean negate) {
    this.negation = negate;
  }

  private OperatorType negateOperator() {
    switch (this.operator) {
      case EQUALTO:
        return OperatorType.NOTEQUAL;

      case NOTEQUAL:
        return OperatorType.EQUALTO;

      case LESSTHAN:
        return OperatorType.GREATERTHANOREQUAL;

      case LESSTHANOREQUAL:
        return OperatorType.GREATERTHAN;

      case GREATERTHAN:
        return OperatorType.LESSTHANOREQUAL;

      case GREATERTHANOREQUAL:
        return OperatorType.LESSTHAN;

      default:
        Utils.log("ERROR :: Invalid operator \"" + this.operator + "\"");
        return OperatorType.INVALID;
    }
  }

}
