import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class AccumuloServiceTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc = new AmcHelper();
}
