package org.example.component;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

public class LogSegment implements AutoCloseable{

    private final ReentrantLock writeLock = new ReentrantLock();
    private final FileChannel fileChannel;
    private final Path logPath;

    public LogSegment(Path partitionDir, String initialFileName) throws IOException {
        logPath = partitionDir.resolve(initialFileName+".log");
        fileChannel = FileChannel.open(logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        fileChannel.position(fileChannel.size());

    }

    public void append(byte[] message) {
        writeLock.lock(); // Ensure only one thread writes to this file at a time
        try {
            // 1. Prepare buffer (Length + Payload)
            ByteBuffer buffer = ByteBuffer.allocateDirect(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            // 2. Write to end of file
            fileChannel.write(buffer, fileChannel.size());

            // fileChannel.force(false);
        } catch (IOException e) {
            e.printStackTrace(); // Handle disk errors gracefully
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * @throws Exception if this resource cannot be closed
     * @apiNote While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     */
    @Override
    public void close() throws Exception {

    }
}
