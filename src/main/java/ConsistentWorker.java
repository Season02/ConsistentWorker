import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConsistentWorker<T> {
    private final Logger logger = LoggerFactory.getLogger(ConsistentWorker.class);

    private ExecutorService workerService;
    private LinkedBlockingQueue<T>[] workerArray;
    private int workerCount;
    private AtomicLong index = new AtomicLong(0);

    ConsistentWorker(int workerCount, String workerName, int queueLength, ConsistenceInterface<T> processor){
        workerArray = new LinkedBlockingQueue[workerCount];
        workerService = Executors.newFixedThreadPool(workerCount);

        for(int i = 0; i < workerCount; i++) {
            workerArray[i] = new LinkedBlockingQueue<T>(queueLength);
            workerService.execute(new EventWorker(processor, i, workerName));
        }

        this.workerCount = workerCount;
    }

    private class EventWorker implements Runnable {
        private Integer index;
        private ConsistenceInterface<T> processor;
        private String workerName;

        private EventWorker(ConsistenceInterface<T> processor, Integer index, String workerName) {
            this.index = index;
            this.processor = processor;
            this.workerName = workerName;
        }

        public void run() {
            Thread.currentThread().setName(workerName + "-" + index);
            while(true){
                try{
                    processor.process(workerArray[index].take());
                }catch (Exception e){
                    logger.warn("Worker error", e);
                }
            }
        }
    }

    public void submit(T data) throws InterruptedException {
        submit(index.getAndIncrement(), data);
    }

    private int indexFor(Object k){
        return null == k ? 0 : (k.hashCode() & Integer.MAX_VALUE) % workerCount;
    }

    public void submit(Object key, T data) throws InterruptedException {
        if(null == data){
            return;
        }

        workerArray[indexFor(key)].put(data);
    }
}
