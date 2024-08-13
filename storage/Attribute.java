package storage;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import utils.ByteConvertor;
import utils.Constants;
import utils.Utils;
import utils.Constants.AttributeStrings;

/**
 * This class denotes each cell of the table, its type and value
 */
public class Attribute {

  // the array that is stored in binary file
  public byte[] fieldValuebyte;
  public Byte[] fieldValueByte;

  // the data type
  public DataType dataType;
  // the field value
  public String fieldValue;

  public Attribute(DataType dataType, byte[] fieldValue) {
    this.dataType = dataType;
    this.fieldValuebyte = fieldValue;

    try {
      switch (dataType) {
        case NULL:
          this.fieldValue = "NULL";
          break;

        case TINYINT:
          this.fieldValue = Byte.valueOf(ByteConvertor.byteFromByteArray(fieldValuebyte)).toString();
          break;

        case SMALLINT:
          this.fieldValue = Short.valueOf(ByteConvertor.shortFromByteArray(fieldValuebyte)).toString();
          break;

        case INT:
          this.fieldValue = Integer.valueOf(ByteConvertor.intFromByteArray(fieldValuebyte)).toString();
          break;

        case BIGINT:
          this.fieldValue = Long.valueOf(ByteConvertor.longFromByteArray(fieldValuebyte)).toString();
          break;

        case FLOAT:
          this.fieldValue = Float.valueOf(ByteConvertor.floatFromByteArray(fieldValuebyte)).toString();
          break;

        case DOUBLE:
          this.fieldValue = Double.valueOf(ByteConvertor.doubleFromByteArray(fieldValuebyte)).toString();
          break;

        case YEAR:
          this.fieldValue = Integer.valueOf((int) ByteConvertor.byteFromByteArray(fieldValuebyte) + 2000).toString();
          break;

        case TIME:
          // Pattern: HH:MM:SS
          int millisSinceMidnight = ByteConvertor.intFromByteArray(fieldValuebyte) % 86400000;
          int seconds = millisSinceMidnight / 1000;
          int hours = seconds / 3600;
          int remHourSeconds = seconds % 3600;
          int minutes = remHourSeconds / 60;
          int remSeconds = remHourSeconds % 60;
          this.fieldValue = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":"
              + String.format("%02d", remSeconds);
          break;

        case DATETIME:
          // Pattern: YYYY-MM-DD_HH:MM:SS
          Date rawDatetime = new Date(ByteConvertor.longFromByteArray(fieldValuebyte));
          this.fieldValue = String.format("%02d", rawDatetime.getYear() + 1900) + "-"
              + String.format("%02d", rawDatetime.getMonth() + 1)
              + "-" + String.format("%02d", rawDatetime.getDate()) + "_" + String.format("%02d", rawDatetime.getHours())
              + ":"
              + String.format("%02d", rawDatetime.getMinutes()) + ":" + String.format("%02d", rawDatetime.getSeconds());
          break;

        case DATE:
          // Pattern: YYYY-MM-DD
          Date rawDate = new Date(ByteConvertor.longFromByteArray(fieldValuebyte));
          this.fieldValue = String.format("%02d", rawDate.getYear() + 1900) + "-"
              + String.format("%02d", rawDate.getMonth() + 1)
              + "-" + String.format("%02d", rawDate.getDate());
          break;

        default:
          this.fieldValue = new String(fieldValuebyte, StandardCharsets.UTF_8);
          break;
      }

      this.fieldValueByte = ByteConvertor.byteToBytes(fieldValuebyte);
    } catch (Exception ex) {
      Utils.log(Constants.AttributeOperations.FORMATTING_ERROR + ex.getMessage());
    }
  }

  public Attribute(DataType dataType, String fieldValue) throws Exception {
    this.dataType = dataType;
    this.fieldValue = fieldValue;

    try {
      switch (dataType) {
        case TEXT:
          this.fieldValuebyte = fieldValue.getBytes();
          break;

        case INT:
          this.fieldValuebyte = ByteConvertor.intTobytes(Integer.parseInt(fieldValue));
          break;

        case SMALLINT:
          this.fieldValuebyte = ByteConvertor.shortTobytes(Short.parseShort(fieldValue));
          break;

        case BIGINT:
          this.fieldValuebyte = ByteConvertor.longTobytes(Long.parseLong(fieldValue));
          break;

        case TINYINT:
          this.fieldValuebyte = new byte[] { Byte.parseByte(fieldValue) };
          break;

        case NULL:
          this.fieldValuebyte = null;
          break;

        case DOUBLE:
          this.fieldValuebyte = ByteConvertor.doubleTobytes(Double.parseDouble(fieldValue));
          break;

        case FLOAT:
          this.fieldValuebyte = ByteConvertor.floatTobytes(Float.parseFloat(fieldValue));
          break;

        case TIME:
          this.fieldValuebyte = ByteConvertor.intTobytes(Integer.parseInt(fieldValue));
          break;

        case YEAR:
          this.fieldValuebyte = new byte[] { (byte) (Integer.parseInt(fieldValue) - 2000) };
          break;

        case DATE:
          SimpleDateFormat sdf = new SimpleDateFormat(AttributeStrings.DATE_FORMAT_STR);
          Date date = sdf.parse(fieldValue);
          this.fieldValuebyte = ByteConvertor.longTobytes(date.getTime());
          break;

        case DATETIME:
          SimpleDateFormat sdftime = new SimpleDateFormat(AttributeStrings.DATETIME_FORMAT_STR);
          Date datetime = sdftime.parse(fieldValue);
          this.fieldValuebyte = ByteConvertor.longTobytes(datetime.getTime());
          break;

        default:
          this.fieldValuebyte = fieldValue.getBytes(StandardCharsets.US_ASCII);
          break;
      }

      this.fieldValueByte = ByteConvertor.byteToBytes(fieldValuebyte);
 
    } catch (Exception e) {
      Utils.log(Constants.AttributeOperations.CONVERSION_ERROR + fieldValue + " to " + dataType.toString());
      throw e;
    }
  }

}
