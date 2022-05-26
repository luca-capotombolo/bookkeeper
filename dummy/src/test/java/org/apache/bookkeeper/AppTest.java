package org.apache.bookkeeper;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void sum()
    {
        App app = new App();
        int c = app.sum(12, 4);
        assertEquals(16, c);

    }

    @Test
    public void sub(){
        App app = new App();
        int c = app.sub(12,4);
        assertEquals(c, 8);
    }

    @Test
    public void mul(){
        App app = new App();
        int c = app.mul(2,2);
        assertEquals(c, 4);
    }

}
