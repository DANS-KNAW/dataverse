package edu.harvard.iq.dataverse.settings;

import edu.harvard.iq.dataverse.api.filter.ApiBlockingFilter;
import edu.harvard.iq.dataverse.util.testing.JvmSetting;
import edu.harvard.iq.dataverse.util.testing.LocalJvmSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@LocalJvmSettings
@JvmSetting(key = JvmSettings.API_BLOCKED_POLICY, value = "")
@JvmSetting(key = JvmSettings.API_BLOCKED_ENDPOINTS, value = "")
class ApiSecurityConfigCheckTest {

    private final ConfigCheckService service = new ConfigCheckService();

    @BeforeEach
    void clearSettings() {
        System.clearProperty(JvmSettings.API_BLOCKED_POLICY.getScopedKey());
        System.clearProperty(JvmSettings.API_BLOCKED_ENDPOINTS.getScopedKey());
    }

    @Test
    void unsetSettingsRetainLegacyFallback() {
        assertDoesNotThrow(service::checkApiSecuritySettings);
    }

    @ParameterizedTest
    @ValueSource(strings = {"drop", "localhost-only", "unblock-key"})
    void acceptsSupportedPolicies(String policy) {
        System.setProperty(JvmSettings.API_BLOCKED_POLICY.getScopedKey(), policy);
        assertDoesNotThrow(service::checkApiSecuritySettings);
    }

    @ParameterizedTest
    @ValueSource(strings = {"DROP", "localhost", "allow", "drop,unblock-key", " drop "})
    void invalidPolicyPreventsStartup(String policy) {
        System.setProperty(JvmSettings.API_BLOCKED_POLICY.getScopedKey(), policy);
        ConfigCheckService.ConfigurationError error =
                assertThrows(ConfigCheckService.ConfigurationError.class, service::startup);
        assertTrue(error.getMessage().contains(JvmSettings.API_BLOCKED_POLICY.getScopedKey()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "admin,builtin-users", " /admin/ , /builtin-users/ ",
            "datasets/{id}/versions/{versionId}/files,files/{id}", "admin/index", "admin/batch/jobs",
            "mydata", "access/datafile/{fileId:.+}"})
    void acceptsExistingPrefixesAndTemplates(String endpoints) {
        System.setProperty(JvmSettings.API_BLOCKED_ENDPOINTS.getScopedKey(), endpoints);
        assertDoesNotThrow(service::checkApiSecuritySettings);
    }

    @ParameterizedTest
    @ValueSource(strings = {"does-not-exist", "admin,does-not-exist", "admin/does-not-exist",
            "files/{id}/does-not-exist", "admins", "admin,", ",admin", "admin,,builtin-users",
            "/", "admin.*", "[", "files/:id", "api/v1/admin"})
    void nonExistentEndpointPreventsStartup(String endpoints) {
        System.setProperty(JvmSettings.API_BLOCKED_ENDPOINTS.getScopedKey(), endpoints);
        ConfigCheckService.ConfigurationError error =
                assertThrows(ConfigCheckService.ConfigurationError.class, service::startup);
        assertTrue(error.getMessage().contains(JvmSettings.API_BLOCKED_ENDPOINTS.getScopedKey()));
    }

    @Test
    void sharedMatcherSupportsTemplatesAndRequiresPathBoundary() {
        assertTrue(ApiBlockingFilter.getBlockedEndpointPattern(" /files/{id}/ ")
                .matcher("files/{id}/metadata").matches());
        assertTrue(ApiBlockingFilter.getBlockedEndpointPattern("access/datafile/{fileId:.+}")
                .matcher("access/datafile/{fileId:.+}").matches());
        assertFalse(ApiBlockingFilter.getBlockedEndpointPattern("admin").matcher("admins/settings").matches());
        assertFalse(ApiBlockingFilter.getBlockedEndpointPattern("admin.*").matcher("admin/settings").matches());
    }
}
