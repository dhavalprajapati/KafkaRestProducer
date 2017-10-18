package org.icc;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.core.impl.provider.entity.ReaderProvider;
import com.sun.jersey.core.impl.provider.entity.StringProvider;
import org.glassfish.jersey.client.ClientResponse;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

// TODO: Need to fix it
public class TestIngestService {
    private JobQueueServer jobQueueServer;
//    @Before
//    public void setup() throws IOException {
//        jobQueueServer = new JobQueueServer();
//        jobQueueServer.init();
//        jobQueueServer.start();
//    }
//
//    @After
//    public void tearDown() {
//        jobQueueServer.stop();
//    }

    @Test
    public void testIngestData() {
        int counter = 0;
        while (counter<=10) {
            try {
                String payload = "{\"clietId\":\"001"+counter+"\",\"transactionAmnt\":\"100"+counter+"\", \"status\":"+null+"\"}";
                counter = counter+1;
                ClientConfig clientConfig = new DefaultClientConfig();
                clientConfig.getClasses().add(StringProvider.class);
                clientConfig.getClasses().add(ReaderProvider.class);
                ClientResponse response = Client.create(clientConfig)
                        .resource("http://0.0.0.0:9999/service/ingest")
                        .post(ClientResponse.class, payload);
                assertEquals("Check Server Status", 200, response.getStatusInfo().getStatusCode());
            } catch(Exception e) {
                continue; // Jersy client throwing exception due to which stops sending another request. Need to add proper dependency to fix it.
            }
        }
    }
}
