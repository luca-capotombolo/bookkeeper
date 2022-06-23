package org.apache.bookkeeper.bookie.mytestbufferedchannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class TestBufferedMock {

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private ByteBuf byteBuf;
    private int sizeWriteBuf;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //capacity read/write buffer, ubb, numberByteToWrite
                {10, 0, 8},             //OK
                {1024, 0, 128},         //OK
                {2048, 0, 1024}         //OK
        });
    }

    public TestBufferedMock(int capacity, int ubb, int sizeWriteBuf) throws IOException {
        configure(capacity, ubb, sizeWriteBuf);
    }

    private void configure(int capacity, int ubb, int sizeWriteBuf) throws IOException {
        //create temporary file
        File log = createTempFile();
        //create file channel
        createFileChannel(log);
        //create buffered channel
        createBufferedChannel(capacity, ubb);
        createByteBuf(sizeWriteBuf);
    }

    private void createByteBuf(int sizeWriteBuf) {
        this.byteBuf = Unpooled.buffer(sizeWriteBuf, sizeWriteBuf);
        byte [] data = new byte[sizeWriteBuf];
        Random random = new Random();
        random.nextBytes(data);
        this.byteBuf.writeBytes(data);
        this.sizeWriteBuf = sizeWriteBuf;
    }

    private File createTempFile() throws IOException {
        File log = File.createTempFile("file", "log");
        log.deleteOnExit();
        return log;
    }

    private void createBufferedChannel(int capacity, int ubb) throws IOException {
        this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, capacity, ubb);
    }

    private void createFileChannel(File log) throws FileNotFoundException {
        this.fileChannel = new RandomAccessFile(log, "rw").getChannel();
    }

    @Test
    public void testWriteReadableBytesMock() {
        Exception error = null;
        ByteBuf buffer = Mockito.mock(ByteBuf.class);
        Mockito.when(buffer.readableBytes()).thenReturn(0);
        try {
            this.bufferedChannel.write(buffer);
        }catch (Exception e){
            error = e;
        }
        Mockito.verify(buffer).readableBytes();
        Assert.assertNull(error);
    }

    @Test
    public void testRead(){
        Exception error = null;
        AtomicInteger count = new AtomicInteger();
        ByteBuf buffer = Mockito.mock(ByteBuf.class);
        Mockito.when(buffer.writeBytes(Mockito.any(ByteBuf.class), Mockito.anyInt(), Mockito.anyInt())).then((Answer<ByteBuf>) invocationOnMock -> {
            Object [] args = invocationOnMock.getArguments();
            count.addAndGet((Integer) args[2]);
            return buffer;
        });
        Mockito.when(buffer.writableBytes()).thenReturn(this.sizeWriteBuf);
        try {
            this.bufferedChannel.write(this.byteBuf);
            this.bufferedChannel.read(buffer, 0, this.sizeWriteBuf);
        }catch (Exception e){
            error = e;
        }
        Mockito.verify(buffer).writeBytes(Mockito.any(ByteBuf.class), Mockito.anyInt(), Mockito.anyInt());
        Assert.assertNull(error);
        Assert.assertEquals(this.sizeWriteBuf, count.get());
    }
}
