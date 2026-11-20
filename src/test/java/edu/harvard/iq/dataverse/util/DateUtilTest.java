package edu.harvard.iq.dataverse.util;

import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DateUtilTest {
    @Test
    void parseEndDateInclusiveUsesEndOfRequestedPrecision() {
        assertEquals("2024-02-29 23:59:59.999", format(DateUtil.parseEndDateInclusive("2024-02-29")));
        assertEquals("2024-02-29 23:59:59.999", format(DateUtil.parseEndDateInclusive("2024-02")));
        assertEquals("2024-12-31 23:59:59.999", format(DateUtil.parseEndDateInclusive("2024")));
    }

    @Test
    void parseEndDateInclusiveRejectsUnsupportedFormats() {
        assertNull(DateUtil.parseEndDateInclusive("2024-02-29T12:00:00"));
    }

    private String format(Date date) {
        return date == null ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
    }
}
