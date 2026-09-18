package edu.harvard.iq.dataverse.pidproviders.doi.crossref;

import edu.harvard.iq.dataverse.pidproviders.PidProvider;
import edu.harvard.iq.dataverse.util.testing.LocalJvmSettings;
import edu.harvard.iq.dataverse.util.testing.SystemProperty;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@LocalJvmSettings
class CrossRefDOIProviderTest {

    @Test
    @SystemProperty(key = "dataverse.pid.crossref.type", value = "crossref")
    @SystemProperty(key = "dataverse.pid.crossref.label", value = "CrossRef")
    @SystemProperty(key = "dataverse.pid.crossref.authority", value = "10.5072")
    @SystemProperty(key = "dataverse.pid.crossref.crossref.url", value = "https://doi.crossref.org")
    @SystemProperty(key = "dataverse.pid.crossref.crossref.rest-api-url", value = "https://api.crossref.org")
    @SystemProperty(key = "dataverse.pid.crossref.crossref.username", value = "user")
    @SystemProperty(key = "dataverse.pid.crossref.crossref.password", value = "pass")
    @SystemProperty(key = "dataverse.pid.crossref.crossref.depositor", value = "test")
    @SystemProperty(key = "dataverse.pid.crossref.crossref.depositor-email", value = "test@example.com")
    @SystemProperty(key = "dataverse.crossref.xml.datafile-info", value = "none")
    void testFactoryCreationWithNoneDatafileInfo() {
        CrossRefDOIProviderFactory factory = new CrossRefDOIProviderFactory();
        PidProvider provider = factory.createPidProvider("crossref");
        assertNotNull(provider);
        assertTrue(provider instanceof CrossRefDOIProvider);
    }
}
