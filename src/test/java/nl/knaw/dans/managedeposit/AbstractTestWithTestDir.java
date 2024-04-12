package nl.knaw.dans.managedeposit;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;

public class AbstractTestWithTestDir {
    protected final Path testDir = Path.of("target/test")
        .resolve(getClass().getSimpleName());

    @BeforeEach
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(testDir.toFile());
    }
}
