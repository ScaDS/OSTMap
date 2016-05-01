import org.junit.Test;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import static org.junit.Assert.assertEquals;


public class TokenSearchTest {
    RestTemplate template = new TestRestTemplate();

    @Test
    public void testRequest() throws Exception {
        ResponseEntity responseEntity = template.getForEntity("http://localhost:8080/tokensearch?field=user,text&token=yolo", String.class);
        HttpStatus status = responseEntity.getStatusCode();
        HttpHeaders httpHeaders = responseEntity.getHeaders();

        assertEquals(MediaType.APPLICATION_JSON_UTF8,httpHeaders.getContentType());
        assertEquals(true,status.is2xxSuccessful());
    }
}
