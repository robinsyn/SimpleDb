package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockManager {
    // integer:list --> 第几页:锁
//    private Map<Integer, List<Lock>> map; //锁表

    // 这里应该用 PageId:list --> 哪个表的第几页:锁
    //LogTest的测试用例TestAbortCommitInterleaved中，对两个表进行插入，第一个表的第1页插入后加了锁，如果是integer:list他们都是第0页，第二个表误以为自己加了锁
    private Map<PageId, List<Lock>> map; //锁表

    public LockManager() {
        this.map = new ConcurrentHashMap<>();

    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions) {
//        Integer pid = pageId.getPageNumber();
        Lock lock = new Lock(permissions, tid);
//        List<Lock> locks = map.get(pid);
        List<Lock> locks = map.get(pageId);
        if (locks == null) {
            locks = new ArrayList<>();
            locks.add(lock);
//            map.put(pid, locks);
            map.put(pageId, locks);
            return true;
        }
        if (locks.size() == 1) {  //只有一个事务占有锁
            Lock firstLock = locks.get(0);
            if (firstLock.getTransactionId().equals(tid)) {
                if (firstLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_WRITE)) {
                    firstLock.setPermissions(Permissions.READ_WRITE); //锁升级
                }
                return true;
            } else {
                if (firstLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_ONLY)) {
                    locks.add(lock);
                    return true;
                }
                return false;
            }
        }
        //list中有多个事务则说明全是共享锁
        if (lock.getPermissions().equals(Permissions.READ_WRITE)) {
            return false;
        }
        //同一个事务重复获取读锁，不要进入列表！
        for (Lock lock1 : locks) {
            if (lock1.getTransactionId().equals(tid)) {
                return true;
            }
        }
        locks.add(lock);
        return true;
    }


    public synchronized void releaseLock(TransactionId transactionId, PageId pageId) {
//        List<Lock> locks = map.get(pageId.getPageNumber());
        List<Lock> locks = map.get(pageId);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            // release lock
            if (lock.getTransactionId().equals(transactionId)) {
                locks.remove(lock);
                if (locks.size() == 0) {
//                    map.remove(pageId.getPageNumber());
                    map.remove(pageId);
                }
                return;
            }
        }
    }

    public synchronized void releaseAllLock(TransactionId transactionId) {
//        for (Integer k : map.keySet()) {
        for (PageId k : map.keySet()) {
            List<Lock> locks = map.get(k);
            for (int i = 0; i < locks.size(); i++) {
                Lock lock = locks.get(i);
                // release lock
                if (lock.getTransactionId().equals(transactionId)) {
                    locks.remove(lock);
                    if (locks.size() == 0) {
                        map.remove(k);
                    }
                    break;
                }
            }
        }
    }

    public synchronized Boolean holdsLock(TransactionId tid, PageId p) {
//        List<Lock> locks = map.get(p.getPageNumber());
        List<Lock> locks = map.get(p);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if (lock.getTransactionId().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
