package storage;

import java.io.*;
import java.util.*;

import utils.Constants.OperatorType;
import utils.Utils;
import utils.Condition;
import utils.Constants;

public class BPlusOneTree {

  // data members
  RandomAccessFile binaryFile;
  int rootPageNum;
  String tableName;

  // constructor
  public BPlusOneTree(RandomAccessFile file, int rootPageNum, String tableName) {
    this.binaryFile = file;
    this.rootPageNum = rootPageNum;
    this.tableName = tableName;
  }

  // method to fetch all leaf pages
  public List<Integer> getAllLeaves() throws IOException {
    List<Integer> leafPages = new ArrayList<>();

    binaryFile.seek((long) rootPageNum * DavisBaseBinaryFile.pageSize);

    Page.PageType rootPageType = Page.PageType.get(binaryFile.readByte());

    if (rootPageType == Page.PageType.LEAF) {
      leafPages.add(rootPageNum);
    } else {
      addLeaves(rootPageNum, leafPages);
    }

    return leafPages;
  }

  // method to add leaf pages
  private void addLeaves(int interiorPageNo, List<Integer> leafPages) throws IOException {
    Page interiorPage = new Page(binaryFile, interiorPageNo);

    for (Page.TableInteriorRecord leftPage : interiorPage.leftChildren) {
      if (Page.getPageType(binaryFile, leftPage.leftChildPageNo) == Page.PageType.LEAF) {
        if (!leafPages.contains(leftPage.leftChildPageNo))
          leafPages.add(leftPage.leftChildPageNo);
      } else {
        addLeaves(leftPage.leftChildPageNo, leafPages);
      }
    }

    if (Page.getPageType(binaryFile, interiorPage.rightPage) == Page.PageType.LEAF) {
      if (!leafPages.contains(interiorPage.rightPage))
        leafPages.add(interiorPage.rightPage);
    } else {
      addLeaves(interiorPage.rightPage, leafPages);
    }
  }

  // method to fetch all leaf pages based on condition
  public List<Integer> getAllLeaves(Condition condition) throws IOException {
    if (condition == null || condition.getOperation() == OperatorType.NOTEQUAL
        || !(new File(Utils.getNDXFilePath(tableName, condition.columnName)).exists())) {

      return getAllLeaves();
    } else {

      RandomAccessFile indexFile = new RandomAccessFile(
          Utils.getNDXFilePath(tableName, condition.columnName), Constants.READ_MODE);
      BTree bTree = new BTree(indexFile);

      List<Integer> row_IDs = bTree.getRowIdValue(condition);
      Set<Integer> hash_Set = new HashSet<>();

      for (int row_ID : row_IDs) {
        hash_Set.add(getPageNum(row_ID, new Page(binaryFile, rootPageNum)));
      }

      Utils.closeIOFile(indexFile);

      return Arrays.asList(hash_Set.toArray(new Integer[0]));
    }
  }

  // method to get page number for insertion
  public static int getPageNumForInsertion(RandomAccessFile file, int rootPageNum) {
    Page rootPage = new Page(file, rootPageNum);
    if (rootPage.pageType != Page.PageType.LEAF && rootPage.pageType != Page.PageType.LEAFINDEX) {
      return getPageNumForInsertion(file, rootPage.rightPage);
    } else {
      return rootPageNum;
    }
  }

  // method to get page number
  public int getPageNum(int row_ID, Page page) {
    if (page.pageType == Page.PageType.LEAF)
      return page.pageNo;

    int index = binarySearchForValue(page.leftChildren, row_ID, 0, page.noOfCells - 1);

    if (row_ID < page.leftChildren.get(index).rowId) {
      return getPageNum(row_ID, new Page(binaryFile, page.leftChildren.get(index).leftChildPageNo));
    } else {
      if (index + 1 < page.leftChildren.size())
        return getPageNum(row_ID, new Page(binaryFile, page.leftChildren.get(index + 1).leftChildPageNo));
      else
        return getPageNum(row_ID, new Page(binaryFile, page.rightPage));
    }
  }

  // method to binary search
  private int binarySearchForValue(List<Page.TableInteriorRecord> vals, int searchVal, int begin, int end) {
    if (end - begin <= 2) {
      int i;

      for (i = begin; i < end; i++) {
        if (vals.get(i).rowId < searchVal) {

        } else {
          break;
        }
      }

      return i;
    } else {
      int mid = (end - begin) / 2 + begin;

      if (vals.get(mid).rowId == searchVal) {
        return mid;
      }

      if (vals.get(mid).rowId < searchVal) {
        return binarySearchForValue(vals, searchVal, mid + 1, end);
      } else {
        return binarySearchForValue(vals, searchVal, begin, mid - 1);
      }
    }
  }

}
