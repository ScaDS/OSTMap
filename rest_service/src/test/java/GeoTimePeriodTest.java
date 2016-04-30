
import org.junit.Test;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;
import static org.junit.Assert.*;

public class GeoTimePeriodTest {
    RestTemplate template = new TestRestTemplate();

    @Test
    public void testRequest() throws Exception {
        HttpStatus status = template.getForEntity("http://localhost:8080/geotemporalsearch?bbnorth=10.123&bbsouth=-10.456&bbeast=-30.789&bbwest=30.123&tstart=1461942000000&tend=1461967200000", String.class).getStatusCode();
        assertEquals(true,status.is2xxSuccessful());
    }
}
