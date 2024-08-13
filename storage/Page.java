package storage;

import java.io.*;
import java.util.*;

import utils.*;

public class Page {

  public PageType pageType;
  short noOfCells = 0;
  public int pageNo;
  short contentStartOffset;
  public int rightPage;
  public int parentPageNo;
  private List<TableRecord> records;
  boolean refreshTableRecords = false;
  long pageStart;
  int lastRowId;
  int availableSpace;
  RandomAccessFile binaryFile;
  List<TableInteriorRecord> leftChildren;

  public DataType indexValueDataType;
  public TreeSet<Long> lIndexValues;
  public TreeSet<String> sIndexValues;
  public HashMap<String, IndexRecord> indexValuePointer;
  private Map<Integer, TableRecord> recordsMap;

  // constructor
  public Page(RandomAccessFile file, int pageNo) {
    try {
      this.pageNo = pageNo;
      indexValueDataType = null;
      lIndexValues = new TreeSet<>();
      sIndexValues = new TreeSet<>();
      indexValuePointer = new HashMap<>();
      recordsMap = new HashMap<>();

      this.binaryFile = file;
      lastRowId = 0;
      pageStart = (long) DavisBaseBinaryFile.pageSize * pageNo;
      binaryFile.seek(pageStart);
      pageType = PageType.get(binaryFile.readByte());
      binaryFile.readByte();
      noOfCells = binaryFile.readShort();
      contentStartOffset = binaryFile.readShort();
      availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

      rightPage = binaryFile.readInt();

      parentPageNo = binaryFile.readInt();

      binaryFile.readShort();

      if (pageType == PageType.LEAF)
        fillTableRecords();
      if (pageType == PageType.INTERIOR)
        fillLeftChildren();
      if (pageType == PageType.INTERIORINDEX || pageType == PageType.LEAFINDEX)
        fillIndexRecords();

    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.PAGE_READ_ERROR + ex.getMessage());
    }
  }

  // to get index values
  public List<String> getIndexValues() {
    List<String> strIndexValues = new ArrayList<>();

    if (sIndexValues.size() > 0)
      strIndexValues.addAll(Arrays.asList(sIndexValues.toArray(new String[0])));
    if (lIndexValues.size() > 0) {
      Long[] lArray = lIndexValues.toArray(new Long[0]);
      for (Long aLong : lArray) {
        strIndexValues.add(aLong.toString());
      }
    }
    return strIndexValues;
  }

  // to get page type
  public static PageType getPageType(RandomAccessFile file, int pageNo) throws IOException {
    try {
      int pageStart = DavisBaseBinaryFile.pageSize * pageNo;
      file.seek(pageStart);
      return PageType.get(file.readByte());
    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.PAGE_FETCH_ERROR + ex.getMessage());
      throw ex;
    }
  }

  // to add new page file
  public static int addNewPage(RandomAccessFile file, PageType pageType, int rightPage, int parentPageNo) {
    try {
      int pageNo = Long.valueOf((file.length() / DavisBaseBinaryFile.pageSize)).intValue();
      file.setLength(file.length() + DavisBaseBinaryFile.pageSize);
      file.seek((long) DavisBaseBinaryFile.pageSize * pageNo);
      file.write(pageType.getValue());
      file.write(0x00);
      file.writeShort(0);
      file.writeShort((short) (DavisBaseBinaryFile.pageSize));

      file.writeInt(rightPage);

      file.writeInt(parentPageNo);
      file.write(0x00);
      file.write(0x00);
      return pageNo;
    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.PAGE_ADD_ERROR + ex.getMessage());
      return -1;
    }
  }

  // to update page record by finding offset
  public void updateRecord(TableRecord record, int ordinalPosition, Byte[] newValue) throws IOException {
    binaryFile.seek(pageStart + record.recordOffset + 7);
    int valueOffset = 0;

    // loop to find the required offset value
    for (int i = 0; i < ordinalPosition; i++) {

      valueOffset += DataType.getLength(binaryFile.readByte());
    }

    binaryFile.seek(pageStart + record.recordOffset + 7 + record.colDataTypes.length + valueOffset);
    // overwrite with new value
    binaryFile.write(ByteConvertor.Bytestobytes(newValue));
  }

  // to add new column to the table
  public void addNewColumn(ColumnInfo columnInfo) {
    try {
      addTableRow(DavisBaseBinaryFile.davisbaseColumns,
          Arrays.asList(new Attribute(DataType.TEXT, columnInfo.tableName),
              new Attribute(DataType.TEXT, columnInfo.columnName),
              new Attribute(DataType.TEXT, columnInfo.dataType.toString()),
              new Attribute(DataType.SMALLINT, columnInfo.ordinalPosition.toString()),
              new Attribute(DataType.TEXT, columnInfo.isNullable ? "YES" : "NO"),
              columnInfo.isPrimaryKey ? new Attribute(DataType.TEXT, "PRI") : new Attribute(DataType.NULL, "NULL"),
              new Attribute(DataType.TEXT, columnInfo.isUnique ? "YES" : "NO")));
    } catch (Exception e) {
      Utils.log(Constants.PageOperations.COLUMN_ADD_ERROR);
    }
  }

  // to add new table entry
  public int addTableRow(String tableName, List<Attribute> attributes) throws IOException {
    List<Byte> colDataTypes = new ArrayList<>();
    List<Byte> recordBody = new ArrayList<>();

    TableMetaData metaData = null;
    if (DavisBaseBinaryFile.dataStoreInitialized) {
      metaData = new TableMetaData(tableName);
      if (!metaData.validateInsert(attributes))
        return -1;
    }

    // loop to add values to the attributes
    for (Attribute attribute : attributes) {

      recordBody.addAll(Arrays.asList(attribute.fieldValueByte));

      if (attribute.dataType == DataType.TEXT) {
        colDataTypes.add(Integer.valueOf(DataType.TEXT.getValue() + (attribute.fieldValue.length())).byteValue());
      } else {
        colDataTypes.add(attribute.dataType.getValue());
      }
    }

    lastRowId++;

    short payLoadSize = Integer.valueOf(recordBody.size() +
        colDataTypes.size() + 1).shortValue();

    List<Byte> recordHeader = new ArrayList<>();

    recordHeader.addAll(Arrays.asList(ByteConvertor.shortToBytes(payLoadSize)));
    recordHeader.addAll(Arrays.asList(ByteConvertor.intToBytes(lastRowId)));
    recordHeader.add(Integer.valueOf(colDataTypes.size()).byteValue());
    recordHeader.addAll(colDataTypes);

    addNewPageRecord(recordHeader.toArray(new Byte[0]),
        recordBody.toArray(new Byte[0]));

    refreshTableRecords = true;
    if (DavisBaseBinaryFile.dataStoreInitialized) {
      assert metaData != null;
      metaData.recordCount++;
      metaData.updateMetaData();
    }

    return lastRowId;
  }

  // to fetch the page records
  public List<TableRecord> getPageRecords() {
    if (refreshTableRecords) {
      fillTableRecords();
    }

    refreshTableRecords = false;

    return records;
  }

  // to delete page records using record index
  private void DeletePageRecord(short recordIndex) {
    try {
      for (int i = recordIndex + 1; i < noOfCells; i++) {
        binaryFile.seek(pageStart + 0x10 + (i * 2L));
        short cellStart = binaryFile.readShort();

        if (cellStart == 0)
          continue;

        binaryFile.seek(pageStart + 0x10 + ((i - 1) * 2L));
        binaryFile.writeShort(cellStart);
      }

      noOfCells--;

      binaryFile.seek(pageStart + 2);
      binaryFile.writeShort(noOfCells);

    } catch (IOException e) {
      Utils.log(Constants.RecordOperations.RECORD_DELETE_ERROR + recordIndex + "in page " + pageNo);
    }
  }

  // to delete table records using record index
  public void DeleteTableRecord(String tableName, short recordIndex) {
    DeletePageRecord(recordIndex);
    TableMetaData metaData = new TableMetaData(tableName);
    metaData.recordCount--;
    metaData.updateMetaData();
    refreshTableRecords = true;
  }

  // to add new page records
  private void addNewPageRecord(Byte[] recordHeader, Byte[] recordBody) throws IOException {

    if (recordHeader.length + recordBody.length + 4 > availableSpace) {
      try {
        if (pageType == PageType.LEAF || pageType == PageType.INTERIOR) {
          handleTableOverFlow();
        } else {
          handleIndexOverflow();
          return;
        }
      } catch (IOException e) {
        Utils.log(Constants.PageOperations.TABLE_OVERFLOW_ERROR);
      }
    }

    short cellStart = contentStartOffset;

    short newCellStart = Integer.valueOf((cellStart - recordBody.length - recordHeader.length - 2)).shortValue();
    binaryFile.seek((long) pageNo * DavisBaseBinaryFile.pageSize + newCellStart);

    binaryFile.write(ByteConvertor.Bytestobytes(recordHeader));

    binaryFile.write(ByteConvertor.Bytestobytes(recordBody));

    binaryFile.seek(pageStart + 0x10 + (noOfCells * 2));
    binaryFile.writeShort(newCellStart);

    contentStartOffset = newCellStart;

    binaryFile.seek(pageStart + 4);
    binaryFile.writeShort(contentStartOffset);

    noOfCells++;
    binaryFile.seek(pageStart + 2);
    binaryFile.writeShort(noOfCells);

    availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);
  }

  private boolean idxPageCleaned;

  // to handle index overflow
  private void handleIndexOverflow() throws IOException {
    if (pageType == PageType.LEAFINDEX) {

      if (parentPageNo == -1) {

        parentPageNo = addNewPage(binaryFile, PageType.INTERIORINDEX, pageNo, -1);
      }

      int newLeftLeafPageNo = addNewPage(binaryFile, PageType.LEAFINDEX, pageNo, parentPageNo);

      setParent(parentPageNo);

      IndexRecord.IndexNode incomingInsertTemp = this.incomingInsert;

      Page leftLeafPage = new Page(binaryFile, newLeftLeafPageNo);

      IndexRecord.IndexNode toInsertParentIndexNode = splitIndexRecordsBetweenPages(leftLeafPage);

      Page parentPage = new Page(binaryFile, parentPageNo);

      int comparisonResult = Condition.compare(incomingInsertTemp.indexValue.fieldValue,
          toInsertParentIndexNode.indexValue.fieldValue, incomingInsert.indexValue.dataType);

      if (comparisonResult == 0) {
        toInsertParentIndexNode.rowids.addAll(incomingInsertTemp.rowids);
        parentPage.addIndex(toInsertParentIndexNode, newLeftLeafPageNo);
        shiftPage(parentPage);
        return;
      } else if (comparisonResult < 0) {
        leftLeafPage.addIndex(incomingInsertTemp);
        shiftPage(leftLeafPage);
      } else {
        addIndex(incomingInsertTemp);
      }

      parentPage.addIndex(toInsertParentIndexNode, newLeftLeafPageNo);
    } else {
      if (noOfCells < 3 && !idxPageCleaned) {
        idxPageCleaned = true;
        String[] indexValuesTemp = getIndexValues().toArray(new String[0]);
        HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) indexValuePointer.clone();
        IndexRecord.IndexNode incomingInsertTemp = this.incomingInsert;

        cleanPage();

        for (String s : indexValuesTemp) {
          addIndex(indexValuePointerTemp.get(s).getIndexNode(), indexValuePointerTemp.get(s).leftPageNo);
        }

        addIndex(incomingInsertTemp);
        return;
      }

      if (idxPageCleaned) {
        Utils.log(Constants.PageOperations.PAGE_OVERFLOW_ERROR);
        return;
      }

      if (parentPageNo == -1) {
        parentPageNo = addNewPage(binaryFile, PageType.INTERIORINDEX, pageNo, -1);
      }

      int newLeftInteriorPageNo = addNewPage(binaryFile, PageType.INTERIORINDEX, pageNo, parentPageNo);

      setParent(parentPageNo);

      IndexRecord.IndexNode incomingInsertTemp = this.incomingInsert;

      Page leftInteriorPage = new Page(binaryFile, newLeftInteriorPageNo);

      IndexRecord.IndexNode toInsertParentIndexNode = splitIndexRecordsBetweenPages(leftInteriorPage);

      Page parentPage = new Page(binaryFile, parentPageNo);

      int comparisonResult = Condition.compare(incomingInsertTemp.indexValue.fieldValue,
          toInsertParentIndexNode.indexValue.fieldValue, incomingInsert.indexValue.dataType);

      Page middleOrphan = new Page(binaryFile, toInsertParentIndexNode.leftPageNo);
      middleOrphan.setParent(parentPageNo);
      leftInteriorPage.setRightPageNo(middleOrphan.pageNo);

      if (comparisonResult == 0) {
        toInsertParentIndexNode.rowids.addAll(incomingInsertTemp.rowids);
        parentPage.addIndex(toInsertParentIndexNode, newLeftInteriorPageNo);
        shiftPage(parentPage);
        return;
      } else if (comparisonResult < 0) {
        leftInteriorPage.addIndex(incomingInsertTemp);
        shiftPage(leftInteriorPage);
      } else {
        addIndex(incomingInsertTemp);
      }

      parentPage.addIndex(toInsertParentIndexNode, newLeftInteriorPageNo);
    }
  }

  // removes previous values and resets the page
  private void cleanPage() throws IOException {

    noOfCells = 0;
    contentStartOffset = Long.valueOf(DavisBaseBinaryFile.pageSize).shortValue();
    availableSpace = contentStartOffset - 0x10;
    byte[] emptybytes = new byte[512 - 16];
    Arrays.fill(emptybytes, (byte) 0);
    binaryFile.seek(pageStart + 16);
    binaryFile.write(emptybytes);
    binaryFile.seek(pageStart + 2);
    binaryFile.writeShort(noOfCells);
    binaryFile.seek(pageStart + 4);
    binaryFile.writeShort(contentStartOffset);
    lIndexValues = new TreeSet<>();
    sIndexValues = new TreeSet<>();
    indexValuePointer = new HashMap<>();

  }

  // to split index records between pages
  private IndexRecord.IndexNode splitIndexRecordsBetweenPages(Page newleftPage) throws IOException {

    try {
      int mid = getIndexValues().size() / 2;
      String[] indexValuesTemp = getIndexValues().toArray(new String[0]);

      IndexRecord.IndexNode toInsertParentIndexNode = indexValuePointer.get(indexValuesTemp[mid]).getIndexNode();
      toInsertParentIndexNode.leftPageNo = indexValuePointer.get(indexValuesTemp[mid]).leftPageNo;

      HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) indexValuePointer.clone();

      for (int i = 0; i < mid; i++) {
        newleftPage.addIndex(indexValuePointerTemp.get(indexValuesTemp[i]).getIndexNode(),
            indexValuePointerTemp.get(indexValuesTemp[i]).leftPageNo);
      }

      cleanPage();
      sIndexValues = new TreeSet<>();
      lIndexValues = new TreeSet<>();
      indexValuePointer = new HashMap<>();

      for (int i = mid + 1; i < indexValuesTemp.length; i++) {
        addIndex(indexValuePointerTemp.get(indexValuesTemp[i]).getIndexNode(),
            indexValuePointerTemp.get(indexValuesTemp[i]).leftPageNo);
      }

      return toInsertParentIndexNode;
    } catch (IOException e) {
      Utils.log(Constants.PageOperations.INDEX_FILE_INSERT_ERROR);
      throw e;
    }
  }

  // to handle table overflow
  private void handleTableOverFlow() throws IOException {
    int newRightLeafPageNo = addNewPage(binaryFile, pageType, -1, -1);
    if (pageType == PageType.LEAF) {

      if (parentPageNo == -1) {

        int newParentPageNo = addNewPage(binaryFile, PageType.INTERIOR,
            newRightLeafPageNo, -1);

        setRightPageNo(newRightLeafPageNo);

        setParent(newParentPageNo);

        Page newParentPage = new Page(binaryFile, newParentPageNo);
        newParentPageNo = newParentPage.addLeftTableChild(pageNo, lastRowId);

        newParentPage.setRightPageNo(newRightLeafPageNo);

        Page newLeafPage = new Page(binaryFile, newRightLeafPageNo);
        newLeafPage.setParent(newParentPageNo);

        shiftPage(newLeafPage);
      } else {

        Page parentPage = new Page(binaryFile, parentPageNo);
        parentPageNo = parentPage.addLeftTableChild(pageNo, lastRowId);

        parentPage.setRightPageNo(newRightLeafPageNo);

        setRightPageNo(newRightLeafPageNo);

        Page newLeafPage = new Page(binaryFile, newRightLeafPageNo);
        newLeafPage.setParent(parentPageNo);

        shiftPage(newLeafPage);
      }
    } else {

      int newParentPageNo = addNewPage(binaryFile, PageType.INTERIOR,
          newRightLeafPageNo, -1);

      setRightPageNo(newRightLeafPageNo);

      setParent(newParentPageNo);

      Page newParentPage = new Page(binaryFile, newParentPageNo);
      newParentPageNo = newParentPage.addLeftTableChild(pageNo, lastRowId);

      newParentPage.setRightPageNo(newRightLeafPageNo);

      Page newLeafPage = new Page(binaryFile, newRightLeafPageNo);
      newLeafPage.setParent(newParentPageNo);

      shiftPage(newLeafPage);
    }
  }

  // adds left children and returns page no
  private int addLeftTableChild(int leftChildPageNo, int rowId) throws IOException {
    for (TableInteriorRecord intRecord : leftChildren) {
      if (intRecord.rowId == rowId)
        return pageNo;
    }

    if (pageType == PageType.INTERIOR) {

      List<Byte> recordHeader = new ArrayList<>(Arrays.asList(ByteConvertor.intToBytes(leftChildPageNo)));
      List<Byte> recordBody = new ArrayList<>(Arrays.asList(ByteConvertor.intToBytes(rowId)));

      addNewPageRecord(recordHeader.toArray(new Byte[0]),
          recordBody.toArray(new Byte[0]));
    }

    return pageNo;
  }

  // to shift existing page to handle overflow
  private void shiftPage(Page newPage) {
    pageType = newPage.pageType;
    noOfCells = newPage.noOfCells;
    pageNo = newPage.pageNo;
    contentStartOffset = newPage.contentStartOffset;
    rightPage = newPage.rightPage;
    parentPageNo = newPage.parentPageNo;
    leftChildren = newPage.leftChildren;
    sIndexValues = newPage.sIndexValues;
    lIndexValues = newPage.lIndexValues;
    indexValuePointer = newPage.indexValuePointer;
    records = newPage.records;
    pageStart = newPage.pageStart;
    availableSpace = newPage.availableSpace;
  }

  // to set parent for a page
  public void setParent(int parentPageNo) throws IOException {
    binaryFile.seek((long) DavisBaseBinaryFile.pageSize * pageNo + 0x0A);
    binaryFile.writeInt(parentPageNo);
    this.parentPageNo = parentPageNo;
  }

  // to set right page
  public void setRightPageNo(int rightPageNo) throws IOException {
    binaryFile.seek((long) DavisBaseBinaryFile.pageSize * pageNo + 0x06);
    binaryFile.writeInt(rightPageNo);
    this.rightPage = rightPageNo;
  }

  // to delete index
  public void DeleteIndex(IndexRecord.IndexNode node) throws IOException {
    DeletePageRecord(indexValuePointer.get(node.indexValue.fieldValue).pageHeaderIndex);
    fillIndexRecords();
    refreshHeaderOffset();
  }

  // to add index using index node
  public void addIndex(IndexRecord.IndexNode node) throws IOException {
    addIndex(node, -1);
  }

  private IndexRecord.IndexNode incomingInsert;

  // to add index given the page number
  public void addIndex(IndexRecord.IndexNode node, int leftPageNo) throws IOException {
    incomingInsert = node;
    incomingInsert.leftPageNo = leftPageNo;
    List<Integer> rowIds = new ArrayList<>();

    getIndexValues();

    if (getIndexValues().contains(node.indexValue.fieldValue)) {
      leftPageNo = indexValuePointer.get(node.indexValue.fieldValue).leftPageNo;
      incomingInsert.leftPageNo = leftPageNo;
      rowIds = indexValuePointer.get(node.indexValue.fieldValue).rowIds;
      rowIds.addAll(incomingInsert.rowids);
      incomingInsert.rowids = rowIds;

      DeletePageRecord(indexValuePointer.get(node.indexValue.fieldValue).pageHeaderIndex);

      if (indexValueDataType == DataType.TEXT || indexValueDataType == null)
        sIndexValues.remove(node.indexValue.fieldValue);
      else
        lIndexValues.remove(Long.parseLong(node.indexValue.fieldValue));
    }

    rowIds.addAll(node.rowids);

    rowIds = new ArrayList<>(new HashSet<>(rowIds));

    List<Byte> recordHead = new ArrayList<>();

    List<Byte> recordBody = new ArrayList<>(List.of(Integer.valueOf(rowIds.size()).byteValue()));

    if (node.indexValue.dataType == DataType.TEXT)
      recordBody.add(Integer.valueOf(node.indexValue.dataType.getValue()
          + node.indexValue.fieldValue.length()).byteValue());
    else
      recordBody.add(node.indexValue.dataType.getValue());

    recordBody.addAll(Arrays.asList(node.indexValue.fieldValueByte));

    for (Integer rowId : rowIds) {
      recordBody.addAll(Arrays.asList(ByteConvertor.intToBytes(rowId)));
    }

    short payload = Integer.valueOf(recordBody.size()).shortValue();
    if (pageType == PageType.INTERIORINDEX)
      recordHead.addAll(Arrays.asList(ByteConvertor.intToBytes(leftPageNo)));

    recordHead.addAll(Arrays.asList(ByteConvertor.shortToBytes(payload)));

    addNewPageRecord(recordHead.toArray(new Byte[0]),
        recordBody.toArray(new Byte[0]));

    fillIndexRecords();
    refreshHeaderOffset();
  }

  // to refresh header offset
  private void refreshHeaderOffset() {
    try {
      binaryFile.seek(pageStart + 0x10);
      for (String indexVal : getIndexValues()) {
        binaryFile.writeShort(indexValuePointer.get(indexVal).pageOffset);
      }

    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.HEADER_OFFSET_REFRESH_ERROR + ex.getMessage());
    }
  }

  // to fill the records
  private void fillTableRecords() {
    short payLoadSize;
    byte noOfcolumns;
    records = new ArrayList<>();
    recordsMap = new HashMap<>();

    try {
      for (short i = 0; i < noOfCells; i++) {
        binaryFile.seek(pageStart + 0x10 + (i * 2));
        short cellStart = binaryFile.readShort();
        if (cellStart == 0)
          continue;
        binaryFile.seek(pageStart + cellStart);

        payLoadSize = binaryFile.readShort();
        int rowId = binaryFile.readInt();
        noOfcolumns = binaryFile.readByte();

        if (lastRowId < rowId)
          lastRowId = rowId;

        byte[] colDatatypes = new byte[noOfcolumns];
        byte[] recordBody = new byte[payLoadSize - noOfcolumns - 1];

        binaryFile.read(colDatatypes);
        binaryFile.read(recordBody);

        TableRecord record = new TableRecord(i, rowId, cellStart, colDatatypes, recordBody);
        records.add(record);
        recordsMap.put(rowId, record);
      }
    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.RECORD_FILL_ERROR + ex.getMessage());
    }
  }

  // to fill the left children of the page
  private void fillLeftChildren() {
    try {
      leftChildren = new ArrayList<>();

      int leftChildPageNo;
      int rowId;
      for (int i = 0; i < noOfCells; i++) {
        binaryFile.seek(pageStart + 0x10 + (i * 2));
        short cellStart = binaryFile.readShort();
        if (cellStart == 0)
          continue;
        binaryFile.seek(pageStart + cellStart);

        leftChildPageNo = binaryFile.readInt();
        rowId = binaryFile.readInt();
        leftChildren.add(new TableInteriorRecord(rowId, leftChildPageNo));
      }
    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.RECORD_FILL_ERROR + ex.getMessage());
    }
  }

  // to fill index records
  private void fillIndexRecords() {
    try {
      lIndexValues = new TreeSet<>();
      sIndexValues = new TreeSet<>();
      indexValuePointer = new HashMap<>();

      int leftPageNo = -1;
      byte noOfRowIds;
      byte dataType;
      for (short i = 0; i < noOfCells; i++) {
        binaryFile.seek(pageStart + 0x10 + (i * 2));
        short cellStart = binaryFile.readShort();
        if (cellStart == 0)
          continue;
        binaryFile.seek(pageStart + cellStart);

        if (pageType == PageType.INTERIORINDEX)
          leftPageNo = binaryFile.readInt();

        binaryFile.readShort();

        noOfRowIds = binaryFile.readByte();
        dataType = binaryFile.readByte();

        if (indexValueDataType == null && DataType.get(dataType) != DataType.NULL)
          indexValueDataType = DataType.get(dataType);

        byte[] indexValue = new byte[DataType.getLength(dataType)];
        binaryFile.read(indexValue);

        List<Integer> lstRowIds = new ArrayList<>();
        for (int j = 0; j < noOfRowIds; j++) {
          lstRowIds.add(binaryFile.readInt());
        }

        IndexRecord record = new IndexRecord(i, DataType.get(dataType), noOfRowIds, indexValue, lstRowIds, leftPageNo,
            rightPage, pageNo, cellStart);

        if (indexValueDataType == DataType.TEXT || indexValueDataType == null)
          sIndexValues.add(record.getIndexNode().indexValue.fieldValue);
        else
          lIndexValues.add(Long.parseLong(record.getIndexNode().indexValue.fieldValue));

        indexValuePointer.put(record.getIndexNode().indexValue.fieldValue, record);

      }
    } catch (IOException ex) {
      Utils.log(Constants.PageOperations.RECORD_FILL_ERROR + ex.getMessage());
    }
  }

  // defines the page type
  public enum PageType {
    INTERIOR((byte) 5),
    INTERIORINDEX((byte) 2),
    LEAF((byte) 13),
    LEAFINDEX((byte) 10);

    private static final Map<Byte, PageType> pageTypeLookup = new HashMap<>();

    static {
      for (PageType s : PageType.values())
        pageTypeLookup.put(s.getValue(), s);
    }

    private final byte value;

    PageType(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return value;
    }

    public static PageType get(byte value) {
      return pageTypeLookup.get(value);
    }
  }

  // to define interior record
  public static class TableInteriorRecord {

    public int rowId;
    public int leftChildPageNo;

    public TableInteriorRecord(int rowId, int leftChildPageNo) {
      this.rowId = rowId;
      this.leftChildPageNo = leftChildPageNo;
    }

  }

  // defines the indexes to the page
  public static class IndexRecord {

    public Byte noOfRowIds;
    public DataType dataType;
    public Byte[] indexValue;
    public List<Integer> rowIds;
    public short pageHeaderIndex;
    public short pageOffset;
    int leftPageNo;
    int rightPageNo;
    int pageNo;
    private final IndexNode indexNode;

    IndexRecord(short pageHeaderIndex, DataType dataType, Byte NoOfRowIds, byte[] indexValue, List<Integer> rowIds,
        int leftPageNo, int rightPageNo, int pageNo, short pageOffset) {

      this.pageOffset = pageOffset;
      this.pageHeaderIndex = pageHeaderIndex;
      this.noOfRowIds = NoOfRowIds;
      this.dataType = dataType;
      this.indexValue = ByteConvertor.byteToBytes(indexValue);
      this.rowIds = rowIds;

      indexNode = new IndexNode(new Attribute(this.dataType, indexValue), rowIds);
      this.leftPageNo = leftPageNo;
      this.rightPageNo = rightPageNo;
      this.pageNo = pageNo;
    }

    public IndexNode getIndexNode() {
      return indexNode;
    }

    public static class IndexNode {
      public Attribute indexValue;
      public List<Integer> rowids;
      public int leftPageNo;

      public IndexNode(Attribute indexValue, List<Integer> rowids) {
        this.indexValue = indexValue;
        this.rowids = rowids;
      }
    }

  }

}
