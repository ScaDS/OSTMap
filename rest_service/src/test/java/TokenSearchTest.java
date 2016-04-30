import org.junit.Test;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;

import static org.junit.Assert.assertEquals;


public class TokenSearchTest {
    RestTemplate template = new TestRestTemplate();

    @Test
    public void testRequest() throws Exception {
        HttpStatus status = template.getForEntity("http://localhost:8080/tokensearch?field=user,text&token=yolo", String.class).getStatusCode();
        assertEquals(true,status.is2xxSuccessful());
    }
}
