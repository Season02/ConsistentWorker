@FunctionalInterface
public interface ConsistenceInterface<T> {
    void process(T data) throws InterruptedException;
}
