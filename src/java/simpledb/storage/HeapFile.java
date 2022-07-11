package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * 不要将所有tuple一次性放入内存
     */

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile rFile = new RandomAccessFile(file, "r");
            rFile.seek(offset);
            for (int i = 0; i < BufferPool.getPageSize(); i++) {
                data[i] = (byte) rFile.read();
            }
            int tableID = pid.getTableId();
            int pageNumber = pid.getPageNumber();
            HeapPageId hpid = new HeapPageId(tableID, pageNumber);
            HeapPage page = new HeapPage(hpid, data);
            rFile.close();
            return page;
        } catch (Exception e) {
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new MyIterator(tid, Permissions.READ_ONLY);
    }


    public class MyIterator implements DbFileIterator {
        TransactionId tid;
        Permissions permissions;
        BufferPool bufferPool = Database.getBufferPool();
        Iterator<Tuple> iterator;
        int num = 0;

        public MyIterator(TransactionId tid, Permissions permissions) {
            this.tid = tid;
            this.permissions = permissions;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {

            num = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), num);

            HeapPage page = (HeapPage) bufferPool.getPage(tid, heapPageId, permissions);
            if (page == null) {
                throw new DbException("null");
            } else {
                iterator = page.iterator();
            }
        }

        public boolean nextPage() throws TransactionAbortedException, DbException {
            while (true) {
                num = num + 1;
                if (num >= numPages()) {
                    return false;
                }
                HeapPageId heapPageId = new HeapPageId(getId(), num);
                HeapPage page = (HeapPage) bufferPool.getPage(tid, heapPageId, permissions);
                if (page == null) {
                    continue;
                }
                iterator = page.iterator();
                if (iterator.hasNext()) {
                    return true;
                }
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return false;
            }
            if (iterator.hasNext()) {
                return true;
            } else {
                return nextPage();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }


}

