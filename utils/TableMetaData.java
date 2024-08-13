package utils;

import storage.Attribute;
import storage.DavisBaseBinaryFile;
import storage.DataType;
import storage.BPlusOneTree;
import storage.Page;
import java.io.*;
import java.util.*;

public class TableMetaData 
{
  public List<TableRecord> columnData;
  public String tableName;
  public boolean doesTableExists;
  public List<ColumnInfo> colNameAttributes;
  public List<String> colNames;
  public int rootPageNum;
  public int recordCount;


  public List<Integer> getOrdinalPositions(List<String> cols) 
  {
    List<Integer> ord_pos=new ArrayList<>();
    for (String col : cols)     
      ord_pos.add(colNames.indexOf(col));    
    return ord_pos;
  }


  public TableMetaData(String tableName) 
  {
    this.tableName = tableName;
    doesTableExists = false;

    try 
    {
      RandomAccessFile davisbase_tables_catalog = new RandomAccessFile(Utils.getTBLFilePath(DavisBaseBinaryFile.davisbaseTables), Constants.READ_MODE);
      int rootPageNum = DavisBaseBinaryFile.getRootPageNo(davisbase_tables_catalog);
      BPlusOneTree b_plus_one_tree = new BPlusOneTree(davisbase_tables_catalog, rootPageNum, tableName);

      for (Integer pageNum : b_plus_one_tree.getAllLeaves()) 
      {
        Page page = new Page(davisbase_tables_catalog, pageNum);
        for (TableRecord rec : page.getPageRecords()) 
        {
          if (rec.getAttributes().get(0).fieldValue.equals(tableName)) 
          {
            this.rootPageNum = Integer.parseInt(rec.getAttributes().get(3).fieldValue);
            doesTableExists = true;
            recordCount = Integer.parseInt(rec.getAttributes().get(1).fieldValue);
            break;
          }
        }
        if (doesTableExists)
          break;
      }

      Utils.closeIOFile(davisbase_tables_catalog);

      if (doesTableExists)       
        loadColumnData();      
      else       
        throw new Exception(Constants.TableMetaData.TABLE_ERROR);
      
    } 
    catch (Exception e)
    { // Caught Exception 
    }
  }


  public boolean columnExists(List<String> cols) 
  {
    if (cols.size() == 0) 
      return true;    

    List<String> lColumns = new ArrayList<>(cols);

    for (ColumnInfo columnNameAttr : colNameAttributes)     
      lColumns.remove(columnNameAttr.columnName);    

    return lColumns.isEmpty();
  }



  public boolean validateInsert(List<Attribute> row) throws IOException 
  {
    RandomAccessFile tab_file;
    tab_file = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_MODE);
    DavisBaseBinaryFile file;
    file = new DavisBaseBinaryFile(tab_file);

    for (int i_val = 0; i_val < colNameAttributes.size(); i_val++) 
    {
      Condition condition = new Condition(colNameAttributes.get(i_val).dataType);
      condition.columnOrdinal = i_val;
      condition.columnName = colNameAttributes.get(i_val).columnName;      
      condition.setOperator(Constants.EQUALS_OP);

      if (colNameAttributes.get(i_val).isUnique) 
      {
        condition.setConditionValue(row.get(i_val).fieldValue);
        if (file.recordExists(this, condition)) 
        {
          Utils.log(Constants.TableMetaData.INSERTION_ERROR_1 + colNameAttributes.get(i_val).columnName + Constants.TableMetaData.INSERTION_ERROR_2);
          Utils.closeIOFile(tab_file);
          return false;
        }
      }
    }
    Utils.closeIOFile(tab_file);
    return true;
  }


  private void loadColumnData() 
  {
    try 
    {
      RandomAccessFile davisbase_columns_catalog = new RandomAccessFile(Utils.getTBLFilePath(DavisBaseBinaryFile.davisbaseColumns), Constants.READ_MODE);
      int rootPageNo = DavisBaseBinaryFile.getRootPageNo(davisbase_columns_catalog);

      columnData = new ArrayList<>();
      colNames = new ArrayList<>();
      colNameAttributes = new ArrayList<>();

      BPlusOneTree b_plus_one_tree = new BPlusOneTree(davisbase_columns_catalog, rootPageNo, tableName);

      for (Integer pno : b_plus_one_tree.getAllLeaves()) 
      {
        Page page;
        page = new Page(davisbase_columns_catalog, pno);

        for (TableRecord rec : page.getPageRecords()) 
        {
          if (rec.getAttributes().get(0).fieldValue.equals(tableName)) 
          {
            ColumnInfo col_info = new ColumnInfo(
                tableName, DataType.get(rec.getAttributes().get(2).fieldValue),
                rec.getAttributes().get(1).fieldValue, rec.getAttributes().get(6).fieldValue.equals("YES"),
                rec.getAttributes().get(4).fieldValue.equals("YES"),
                Short.parseShort(rec.getAttributes().get(3).fieldValue));
            
            columnData.add(rec);
            colNames.add(rec.getAttributes().get(1).fieldValue);

            if (rec.getAttributes().get(5).fieldValue.equals("PRI"))            
              col_info.setAsPrimaryKey();           

            colNameAttributes.add(col_info);
          }
        }
      }
      Utils.closeIOFile(davisbase_columns_catalog);
    } 
    catch (Exception e) 
    { Utils.log(Constants.TableMetaData.COLUMN_DATA_ERROR + tableName); }

  }


  public void updateMetaData() 
  {
    try 
    {
      RandomAccessFile tab_file = new RandomAccessFile(Utils.getTBLFilePath(tableName), Constants.READ_MODE);
      int rootPageNo = DavisBaseBinaryFile.getRootPageNo(tab_file);
      Utils.closeIOFile(tab_file);

      RandomAccessFile davisbase_tables_catalog = new RandomAccessFile(Utils.getTBLFilePath(DavisBaseBinaryFile.davisbaseTables), Constants.READ_WRITE_MODE);
      DavisBaseBinaryFile tables_binary_file = new DavisBaseBinaryFile(davisbase_tables_catalog);
      TableMetaData tables_meta_data = new TableMetaData(DavisBaseBinaryFile.davisbaseTables);

      Condition condition = new Condition(DataType.TEXT);
      condition.columnOrdinal = 0;
      condition.setColumName("table_name");
      condition.setConditionValue(tableName);
      condition.setOperator(Constants.EQUALS_OP);

      List<String> cols = Arrays.asList("record_count", "root_page");
      List<String> new_vals = new ArrayList<>();

      new_vals.add(Integer.toString(recordCount));
      new_vals.add(Integer.toString(rootPageNo));

      tables_binary_file.updateRecords(tables_meta_data, condition, cols, new_vals);
      Utils.closeIOFile(davisbase_tables_catalog);
    } 
    catch (IOException e) 
    { Utils.log(Constants.TableMetaData.META_DATA_ERROR + tableName); }
  } 

}
