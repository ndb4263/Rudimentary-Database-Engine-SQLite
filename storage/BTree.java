package storage;

import java.io.*;
import java.util.*;

import utils.Constants;
import utils.Utils;
import utils.Condition;
import utils.Constants.OperatorType;

/**
 * This class contains the logic to the data structure
 * to store pages and indexes
 */
public class BTree {
  Page root;
  RandomAccessFile binaryFile;

  public BTree(RandomAccessFile file) {
    this.binaryFile = file;
    this.root = new Page(binaryFile, DavisBaseBinaryFile.getRootPageNo(binaryFile));
  }

  // to get the closest page num to the current page
  private int getClosestPageNumber(Page page, String value) {
    if (page.pageType == Page.PageType.LEAFINDEX) {
      return page.pageNo;
    } else {
      if (Condition.compare(value, page.getIndexValues().get(0), page.indexValueDataType) < 0) {
        return getClosestPageNumber(
            new Page(binaryFile, page.indexValuePointer.get(page.getIndexValues().get(0)).leftPageNo),
            value);
      } else if (Condition.compare(value, page.getIndexValues().get(page.getIndexValues().size() - 1),
          page.indexValueDataType) > 0) {
        return getClosestPageNumber(new Page(binaryFile, page.rightPage), value);
      } else {

        String closestValue = binarySearchForValue(page.getIndexValues().toArray(new String[0]), value, 0,
            page.getIndexValues().size() - 1, page.indexValueDataType);

        int i = page.getIndexValues().indexOf(closestValue);
        List<String> indexValues = page.getIndexValues();

        if (closestValue.compareTo(value) < 0 && i + 1 < indexValues.size()) {
          return page.indexValuePointer.get(indexValues.get(i + 1)).leftPageNo;
        } else if (closestValue.compareTo(value) > 0) {
          return page.indexValuePointer.get(closestValue).leftPageNo;
        } else {
          return page.pageNo;
        }
      }
    }
  }

  // to get the row ids of all the pages that matches the condition
  public List<Integer> getRowIdValue(Condition condition) {
    List<Integer> rowIds = new ArrayList<>();

    Page page = new Page(binaryFile, getClosestPageNumber(root, condition.comparisonValue));

    String[] indexValues = page.getIndexValues().toArray(new String[0]);

    OperatorType operType = condition.getOperation();

    for (String indexValue : indexValues) {
      if (condition.checkCondition(page.indexValuePointer.get(indexValue).getIndexNode().indexValue.fieldValue))
        rowIds.addAll(page.indexValuePointer.get(indexValue).rowIds);
    }

    if (operType == OperatorType.LESSTHAN || operType == OperatorType.LESSTHANOREQUAL) {
      if (page.pageType == Page.PageType.LEAFINDEX)
        rowIds.addAll(getAllRowIdsLeftOf(page.parentPageNo, indexValues[0]));
      else
        rowIds.addAll(getAllRowIdsLeftOf(page.pageNo, condition.comparisonValue));
    }

    ;

    if (operType == OperatorType.GREATERTHAN || operType == OperatorType.GREATERTHANOREQUAL) {
      if (page.pageType == Page.PageType.LEAFINDEX)
        rowIds.addAll(getAllRightRowIds(page.parentPageNo, indexValues[indexValues.length - 1]));
      else
        rowIds.addAll(getAllRightRowIds(page.pageNo, condition.comparisonValue));
    }

    return rowIds;
  }

  // to get all row ids left of the current page
  private List<Integer> getAllRowIdsLeftOf(int pageNo, String indexValue) {
    List<Integer> rowIds = new ArrayList<>();

    if (pageNo == -1) {
      return rowIds;
    }

    Page page = new Page(this.binaryFile, pageNo);
    List<String> indexValues = Arrays.asList(page.getIndexValues().toArray(new String[0]));

    for (int i = 0; i < indexValues.size()
        && Condition.compare(indexValues.get(i), indexValue, page.indexValueDataType) < 0; i++) {

      rowIds.addAll(page.indexValuePointer.get(indexValues.get(i)).getIndexNode().rowids);
      addChildRowIds(page.indexValuePointer.get(indexValues.get(i)).leftPageNo, rowIds);

    }

    if (page.indexValuePointer.get(indexValue) != null)
      addChildRowIds(page.indexValuePointer.get(indexValue).leftPageNo, rowIds);

    return rowIds;
  }

  // to get all the row ids
  private List<Integer> getAllRightRowIds(int pageNo, String indexValue) {
    List<Integer> rowIds = new ArrayList<>();

    if (pageNo == -1) {
      return rowIds;
    }

    Page page = new Page(this.binaryFile, pageNo);
    List<String> indexValues = Arrays.asList(page.getIndexValues().toArray(new String[0]));

    int indexValSize = indexValues.size() - 1;
    for (int i = indexValSize; i >= 0
        && Condition.compare(indexValues.get(i), indexValue, page.indexValueDataType) > 0; i--) {
      rowIds.addAll(page.indexValuePointer.get(indexValues.get(i)).getIndexNode().rowids);
      addChildRowIds(page.rightPage, rowIds);
    }

    if (page.indexValuePointer.get(indexValue) != null) {
      addChildRowIds(page.indexValuePointer.get(indexValue).rightPageNo, rowIds);
    }

    return rowIds;
  }

  // fetches and adds all the row ids of its children
  private void addChildRowIds(int pageNo, List<Integer> rowIds) {
    if (pageNo == -1) {
      return;
    }

    Page page = new Page(this.binaryFile, pageNo);

    // loop through all page index records
    for (Page.IndexRecord record : page.indexValuePointer.values()) {
      rowIds.addAll(record.rowIds);
      if (page.pageType == Page.PageType.INTERIORINDEX) {
        addChildRowIds(record.leftPageNo, rowIds);
        addChildRowIds(record.rightPageNo, rowIds);
      }
    }
  }

  // to insert the page index node
  public void insert(Attribute attribute, List<Integer> rowIds) {
    try {
      int pageNo = getClosestPageNumber(root, attribute.fieldValue);
      Page page = new Page(binaryFile, pageNo);
      page.addIndex(new Page.IndexRecord.IndexNode(attribute, rowIds));
    } catch (IOException e) {
      Utils.log(Constants.PageOperations.ATTRIBUTE_INSERT_ERROR + attribute.fieldValue);
    }
  }

  public void insert(Attribute attribute, int rowId) {
    insert(attribute, List.of(rowId));
  }

  // to delete index for page with this attribute and row id
  public void delete(Attribute attribute, int rowId) {
    try {
      int pageNo = getClosestPageNumber(root, attribute.fieldValue);
      Page page = new Page(binaryFile, pageNo);

      Page.IndexRecord.IndexNode tempNode = page.indexValuePointer.get(attribute.fieldValue).getIndexNode();

      tempNode.rowids.remove((Integer) rowId);

      page.DeleteIndex(tempNode);

      if (tempNode.rowids.size() != 0) {
        page.addIndex(tempNode);
      }
    } catch (IOException e) {
      Utils.log(Constants.PageOperations.ATTRIBUTE_DELETE_ERROR + attribute.fieldValue);
    }
  }

  private String binarySearchForValue(String[] values, String searchValue, int start, int end, DataType dataType) {
    if (end - start <= 3) {
      int i;
      i = start;
      while (i < end) {
        if (Condition.compare(values[i], searchValue, dataType) < 0) {
        } else {
          break;
        }
        i++;
      }

      return values[i];
    } else {
      int mid = (end - start) / 2 + start;

      if (values[mid].equals(searchValue)) {
        return values[mid];
      }

      if (Condition.compare(values[mid], searchValue, dataType) < 0) {
        return binarySearchForValue(values, searchValue, mid + 1, end, dataType);
      } else {
        return binarySearchForValue(values, searchValue, start, mid - 1, dataType);
      }
    }
  }

}
