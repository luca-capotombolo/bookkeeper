package org.apache.bookkeeper.bookie.mytestbookieimpl;

import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestGetBookieAddressFailure {

    private ServerConfiguration serverConfiguration;

    public TestGetBookieAddressFailure(String advertisedAddress, int portNumber, String listeningInterface, boolean useHostNameAsID, boolean useShortHN, boolean loopBack){
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

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                {"", -1, "eth0", false, false, false},
                {null, 1025, "it is not a network interface", false, false, false},
                {"", -1000, null, false, false, false},
                {"", 65536, "eth1", false, false, false},
                //next iteration
                {"192.168.1.40", -1, "eth0", false, false, false}
        });
    }

    @Test
    public void testFailure(){
        Exception error = null;

        try {
            BookieImpl.getBookieAddress(this.serverConfiguration);
        }catch (Exception e){
            error = e;
        }

        Assert.assertNotNull(error);
    }
}
