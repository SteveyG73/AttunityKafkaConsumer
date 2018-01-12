package steveyg.kafka.attunity;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Signal class used to let the consumer thread know the application has been shut down
 */

class Signal {
    ReadWriteLock lock = new ReentrantReadWriteLock();
    volatile boolean running = true;

    /**
     * Sets the running flag to false
     */
    public void stop() {
        lock.writeLock().lock();
        try {
            running = false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }
    /**
     * Check to see if the signal to stop has happened
     *
     * @return boolean has the signal to stop been made?
     */
    public boolean isStopped() {
        boolean runningLocal;
        lock.readLock().lock();
        try {
            runningLocal = this.running;
        }
        finally {
            lock.readLock().unlock();
        }

        return !running;
    }
}
