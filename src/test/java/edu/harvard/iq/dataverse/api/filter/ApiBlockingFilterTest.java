package edu.harvard.iq.dataverse.api.filter;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApiBlockingFilterTest {

    @ParameterizedTest
    @CsvSource({
        "api/admin, admin",
        "/api/admin/, admin",
        "apiary/admin, apiary/admin"
    })
    void canonicalizeRemovesRedundantApiPrefix(String endpoint, String expected) {
        assertEquals(expected, ApiBlockingFilter.canonicalize(endpoint));
    }
}
