package org.apache.bookkeeper.bookie.mytestbufferedchannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class TestBufferedChannelWriteSuccess {

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private ByteBuf byteBuf;
    private int sizeWriteBuf;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //capacity read/write buffer, ubb, numberByteToWrite
                {10000, 0, 8192},           //OK
                {2048, 0, 512},             //OK
                //empty buffer
                {512, 0, 0},                //OK
                //next iteration
                {256, 1, 128},               //OK
                {256, 0, 512}
        });
    }

    public TestBufferedChannelWriteSuccess(int capacity, int ubb, int sizeWriteBuf) throws IOException {
        configure(capacity, ubb, sizeWriteBuf);
    }

    private void configure(int capacity, int ubb, int sizeWriteBuf) throws IOException {
        File log = createTempFile();
        createFileChannel(log);
        createBufferedChannel(capacity, ubb);
        createByteBuf(sizeWriteBuf);
    }

    private void createByteBuf(int sizeWriteBuf) throws IOException {
        this.byteBuf = Unpooled.buffer(sizeWriteBuf, sizeWriteBuf);
        byte [] data = new byte[sizeWriteBuf];
        Random random = new Random();
        random.nextBytes(data);
        byteBuf.writeBytes(data);
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
    public void testWriteWithSuccess() throws IOException {
        Exception error = null;
        ByteBuf buffer = Unpooled.buffer(this.sizeWriteBuf, this.sizeWriteBuf);
        try {
            this.bufferedChannel.write(this.byteBuf);
        }catch (Exception e){
            error = e;
        }
        Assert.assertNull(error);

        int n = this.bufferedChannel.read(buffer, 0);
        Assert.assertEquals(this.sizeWriteBuf, n);
    }
}
