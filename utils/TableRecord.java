package utils;

import java.util.*;

import storage.Attribute;
import storage.DataType;

// class for converting table data into byte array
public class TableRecord {

  public int rowId;
  public Byte[] colDataTypes;
  public Byte[] recordBody;
  private List<Attribute> attributes;
  public short recordOffset;
  public short pageHeaderIndex;

  public TableRecord(short pageHeaderIndex, int rowId, short recordOffset, byte[] colDataTypes, byte[] recordBody) {
    this.rowId = rowId;
    this.recordBody = ByteConvertor.byteToBytes(recordBody);
    this.colDataTypes = ByteConvertor.byteToBytes(colDataTypes);
    this.recordOffset = recordOffset;
    this.pageHeaderIndex = pageHeaderIndex;
    setAttributes();
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  private void setAttributes() {
    attributes = new ArrayList<>();
    int pointer = 0;
    for (Byte colDataType : colDataTypes) {
      byte[] fieldValue = ByteConvertor
          .Bytestobytes(Arrays.copyOfRange(recordBody, pointer, pointer + DataType.getLength(colDataType)));
      attributes.add(new Attribute(DataType.get(colDataType), fieldValue));
      pointer = pointer + DataType.getLength(colDataType);
    }
  }

}
