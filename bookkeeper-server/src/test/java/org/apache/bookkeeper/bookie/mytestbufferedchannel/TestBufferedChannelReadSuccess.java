package org.apache.bookkeeper.bookie.mytestbufferedchannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class TestBufferedChannelReadSuccess {

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private ByteBuf readBuf;
    private int pos;
    private int length;
    private int expected;


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //capacity read/write buffer, ubb, number of bytes I write in the channel, number of bytes of read buffer, pos (read), length (read)
                {2048, 0, 256, 512, 0, 256},
                {256, 0, 256, 256, 0, 256},
                {2048, 0, 256, 0, 0, 0}
                //{2048, 0, 256, 512, 250, 3} strano
                //next iteration
                //{2048, 0, 2048, 256, 0, 256},
                //{2048, 0, 4096, 256, 0, 256}
        });
    }

    public TestBufferedChannelReadSuccess(int capacity, int ubb, int numberByteWritten, int sizeReadBuf, int pos, int length) throws IOException {
        configure(capacity, ubb, numberByteWritten, sizeReadBuf, pos, length);
    }

    private void configure(int capacity, int ubb, int numberByteWritten, int sizeReadBuf, int pos, int length) throws IOException {
        File log = createTempFile();
        createFileChannel(log);
        createBufferedChannel(capacity, ubb);
        writeChannel(numberByteWritten);
        createReadByteBuf(sizeReadBuf);
        this.pos = pos;
        this.length = length;
        this.expected = length - pos;
    }

    private void writeChannel(int numberByteWritten) throws IOException {
        ByteBuf writeBuf = Unpooled.buffer(numberByteWritten, numberByteWritten);
        byte [] data = new byte[numberByteWritten];
        Random random = new Random();
        random.nextBytes(data);
        writeBuf.writeBytes(data);
        this.bufferedChannel.write(writeBuf);
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

    private void createReadByteBuf(int sizeReadBuf) {
        this.readBuf = Unpooled.buffer(sizeReadBuf, sizeReadBuf);
    }

    @Test
    public void testReadAll() throws IOException {
        int n = this.bufferedChannel.read(readBuf, this.pos, this.length);
        Assert.assertEquals(this.expected, n);
    }
}
