package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

//    private final ConcurrentHashMap<PageId, Page> map;

    private LRUCache<PageId, Page> lruCache;

    private final LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {

        // some code goes here
        this.numPages = numPages;
        this.lruCache = new LRUCache<>(numPages);
//        map = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
//        Page page = map.get(pid);
//        if (page == null) {
//            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//            addToBufferPool(pid, page);
//        }
//
//        return page;

        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000);
        while (!lockAcquired) {
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new TransactionAbortedException();
            }
            lockAcquired = lockManager.acquireLock(tid, pid, perm);
        }

        if (lruCache.get(pid) == null) {
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = databaseFile.readPage(pid);
            addToBufferPool(pid, page);
            return page;

        } else {
            return lruCache.get(pid);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            rollback(tid);
        }
        lockManager.releaseAllLock(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
//        List<Page> pageList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
//        for (Page page : pageList) {
//            addToBufferPool(page.getId(), page);
//        }
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        //System.out.println(tid.getId());
        List<Page> pages = databaseFile.insertTuple(tid, t);
        //System.out.println(tid.getId());
        for (Page page : pages) {    //???????????????buffer???????????????
            page.markDirty(true, tid);
            addToBufferPool(page.getId(), page);
        }
    }

    /**
     * load page into buffer pool, if buffer pool is full, evict a page
     *
     * @param id
     * @param page
     * @throws DbException
     */
    private void addToBufferPool(PageId id, Page page) throws DbException {
        if (lruCache.getSize() >= this.numPages) {
            evictPage();
        }
        lruCache.put(id, page);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
//        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
//        dbFile.deleteTuple(tid, t);
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (int i = 0; i < pages.size(); i++) {
            pages.get(i).markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
        while (head != tail) {
            Page value = head.value;
            if (value != null && value.isDirty() != null && value.isDirty().equals(tid)) {
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(value.getId().getTableId());
                try {
                    Database.getLogFile().logWrite(value.isDirty(), value.getBeforeImage(), value);
                    Database.getLogFile().force();
                    value.markDirty(false, null);
                    databaseFile.writePage(value);
                    value.setBeforeImage();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            head = head.next;
        }
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
//        assert map.size() == numPages;
//        for (Page page : map.values()) {
//            if (page.isDirty() == null) { // no steal page is dirty, load to disk
//                map.remove(page.getId());
//                return;
//            }
//        }
//        throw new DbException("No Clean Page to EVICT");
        // not necessary for lab1
        //??????????????????????????????
        Page value = lruCache.getTail().prev.value;
        //  ???????????????
        if (value != null && value.isDirty() != null) {
            findNotDirty();
        } else {
            //??????????????????????????????????????????
            lruCache.discard();
        }
    }

    private void findNotDirty() throws DbException {
        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
        tail = tail.prev;
        while (head != tail) {
            Page value = tail.value;
            if (value != null && value.isDirty() == null) {
                lruCache.remove(tail);
                return;
            }
            tail = tail.prev;
        }
        throw new DbException("no dirty page");
    }

    private synchronized void rollback(TransactionId transactionId) {
        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
        while (head != tail) {
            Page value = head.value;
            LRUCache<PageId, Page>.DLinkedNode tmp = head.next;
            if (value != null && value.isDirty() != null && value.isDirty().equals(transactionId)) {
                //????????????
                lruCache.remove(head);
                try {
                    //?????????????????????
                    Page page = Database.getBufferPool().getPage(transactionId, value.getId(), Permissions.READ_ONLY);
                    page.markDirty(false,null);
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                }
            }
            head = tmp;
        }
    }



}
