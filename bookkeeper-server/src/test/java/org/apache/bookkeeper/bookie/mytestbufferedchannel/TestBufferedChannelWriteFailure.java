package org.apache.bookkeeper.bookie.mytestbufferedchannel;

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

@RunWith(Parameterized.class)
public class TestBufferedChannelWriteFailure {

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                //capacity read/write buffer, ubb
                {2048, 0},
                {1024, 1}
        });
    }

    public TestBufferedChannelWriteFailure(int capacity, int ubb) throws IOException {
        configure(capacity, ubb);
    }

    private void configure(int capacity, int ubb) throws IOException {
        File log = createTempFile();
        createFileChannel(log);
        createBufferedChannel(capacity, ubb);
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
    public void testNull(){
        Exception error = null;

        try{
            this.bufferedChannel.write(null);
        }catch (Exception e){
            error = e;
        }

        Assert.assertNotNull(error);
    }
}

