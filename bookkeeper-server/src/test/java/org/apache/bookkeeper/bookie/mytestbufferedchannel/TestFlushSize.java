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
public class TestFlushSize {

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private File log;
    private int numberByteWritten;

    public TestFlushSize(int capacity, int ubb, int sizeWriteBuf) throws IOException {
        configure(capacity, ubb, sizeWriteBuf);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //capacity read/write buffer, ubb, number byte to write
                {2048, 0, 512},             //OK
                {8192, 0, 2048},            //OK
                {4096, 0, 256},             //OK
                {2, 0, 1}                   //OK
        });
    }

    private void configure(int capacity, int ubb, int sizeWriteBuf) throws IOException {
        this.log = createTempFile();
        this.numberByteWritten = sizeWriteBuf;
        createFileChannel(this.log);
        createBufferedChannel(capacity, ubb);
        createByteBuf(sizeWriteBuf);
    }

    private void createByteBuf(int sizeWriteBuf) throws IOException {
        ByteBuf byteBuf = Unpooled.buffer(sizeWriteBuf, sizeWriteBuf);
        byte [] data = new byte[sizeWriteBuf];
        Random random = new Random();
        random.nextBytes(data);
        byteBuf.writeBytes(data);
        this.bufferedChannel.write(byteBuf);
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
    public void testFlush(){

        Exception error = null;

        Assert.assertEquals(this.numberByteWritten, this.bufferedChannel.getNumOfBytesInWriteBuffer());

        try {
            this.bufferedChannel.flush();
        }catch (Exception e1){
            error = e1;
        }

        Assert.assertNull(error);

        Assert.assertEquals(0, this.bufferedChannel.getNumOfBytesInWriteBuffer());

        Assert.assertEquals(this.numberByteWritten, this.log.length());

        Assert.assertEquals(this.numberByteWritten, this.bufferedChannel.position());


        try {
            this.bufferedChannel.close();
        }catch (Exception e2){
            error = e2;
        }

        Assert.assertNull(error);

        try {
            this.bufferedChannel.close();
        }catch (Exception e2){
            error = e2;
        }

        Assert.assertNull(error);

    }
}
