package org.modeshape.connector.disk;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.modeshape.graph.ExecutionContext;

public class FineGrainedLockingDiskTransaction extends DiskTransaction {

    private final Set<Lock> heldLocks = new HashSet<Lock>();

    public FineGrainedLockingDiskTransaction( ExecutionContext context,
                                              DiskRepository repository,
                                              UUID rootNodeUuid ) {
        super(context, repository, rootNodeUuid, null);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void commit() {
        super.commit();
        releaseLocks();
    }

    @Override
    public void rollback() {
        super.rollback();
        releaseLocks();
    }

    private void releaseLocks() {
        for (Lock lock : heldLocks) {
            lock.unlock();
        }
    }

    @Override
    public DiskWorkspace getWorkspace( String workspaceName,
                                       boolean readonly ) {
        DiskWorkspace workspace = super.getWorkspace(workspaceName, readonly);
        if (workspace == null) return null;
        
        Lock lock = readonly ? workspace.readLock() : workspace.writeLock();
        
        try {
            lock.lock();
        } catch (RuntimeException t) {
            t.printStackTrace();
            throw t;
        }

        heldLocks.add(lock);
        
        return workspace;
    }

}
