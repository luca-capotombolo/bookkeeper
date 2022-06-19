package org.apache.bookkeeper.bookie.mytestbookieimpl;

import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

//@RunWith(Parameterized.class)
public class TestGetBookieAddressAdvertised {

    private ServerConfiguration serverConfiguration;

    public TestGetBookieAddressAdvertised(String advertisedAddress, int portNumber, String listeningInterface, boolean useHostNameAsID, boolean useShortHN, boolean loopBack){
        configure(advertisedAddress, portNumber, listeningInterface, useHostNameAsID, useShortHN, loopBack);
    }

    private void configure(String advertisedAddress, int portNumber, String listeningInterface, boolean useHostNameAsID, boolean useShortHN, boolean loopBack) {
        this.serverConfiguration = new ServerConfiguration();
        this.serverConfiguration.setAdvertisedAddress(advertisedAddress);
        this.serverConfiguration.setBookiePort(portNumber);
        this.serverConfiguration.setListeningInterface(listeningInterface);
        this.serverConfiguration.setUseHostNameAsBookieID(useHostNameAsID);
        this.serverConfiguration.setUseShortHostName(useShortHN);
        this.serverConfiguration.setAllowLoopback(loopBack);
    }

    //@Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                {"192.168.1.40", 1025, "eth0", false, false, false},
                {"192.168.1.40", 65535, " it is not a network interface", false, false, false},
                {"", 65535, "eth2", true, false, false},
                {"", 5000, "eth1", true, true, false}
        });
    }

    //@Test
    public void testSuccess(){
        Exception error = null;

        try {
            BookieImpl.getBookieAddress(this.serverConfiguration);
        }catch (Exception e){
            error = e;
        }

        Assert.assertNull(error);
    }


    //@Test
    public void testCheckBookieSocketAddress() throws UnknownHostException {
        BookieSocketAddress bookieSocketAddress;
        BookieSocketAddress expected;

        expected = new BookieSocketAddress(this.serverConfiguration.getAdvertisedAddress(), this.serverConfiguration.getBookiePort());
        bookieSocketAddress = BookieImpl.getBookieAddress(this.serverConfiguration);

        if(Objects.equals(this.serverConfiguration.getAdvertisedAddress(), "") && this.serverConfiguration.getUseHostNameAsBookieID())
            Assert.assertEquals("MSI", bookieSocketAddress.getHostName());

        if(!Objects.equals(this.serverConfiguration.getAdvertisedAddress(), "") && this.serverConfiguration.getAdvertisedAddress()!=null)
            Assert.assertEquals(expected, bookieSocketAddress);

        Assert.assertEquals(expected.getPort(), bookieSocketAddress.getPort());
    }


}