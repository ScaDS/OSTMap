package org.iidp.ostmap.rest_service;

import org.iidp.ostmap.rest_service.MainController;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(MainController.class)
@WebIntegrationTest(randomPort = true)
public class TokenSearchTest {
    RestTemplate template = new TestRestTemplate();

    /**
     * Get the random chosen port number.
     */
    @Value("${local.server.port}")
    int port;

    /*
    @Test
    public void testRequest() throws Exception {
        String url = "http://localhost:" + port + "/api/tokensearch?field=user,text&token=yolo";
        ResponseEntity responseEntity = template.getForEntity(url, String.class);
        HttpStatus status = responseEntity.getStatusCode();
        HttpHeaders httpHeaders = responseEntity.getHeaders();

        assertEquals(MediaType.APPLICATION_JSON_UTF8,httpHeaders.getContentType());
        assertEquals(true,status.is2xxSuccessful());
    }
    */


}
