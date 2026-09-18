package edu.harvard.iq.dataverse.pidproviders.doi.ezid;

import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetField;
import edu.harvard.iq.dataverse.DatasetFieldConstant;
import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.DatasetVersion.VersionState;
import edu.harvard.iq.dataverse.GlobalId;
import edu.harvard.iq.dataverse.TermsOfUseAndAccess;
import edu.harvard.iq.dataverse.dataset.DatasetType;
import edu.harvard.iq.dataverse.pidproviders.PidProvider;
import edu.harvard.iq.dataverse.pidproviders.PidProviderFactoryBean;
import edu.harvard.iq.dataverse.pidproviders.doi.XmlMetadataTemplate.DatafileInfoMode;
import edu.harvard.iq.dataverse.util.SystemConfig;
import edu.harvard.iq.dataverse.util.testing.LocalJvmSettings;
import edu.harvard.iq.dataverse.util.testing.SystemProperty;
import io.restassured.path.xml.XmlPath;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@LocalJvmSettings
class EZIdDOIProviderTest {

    @Test
    @SystemProperty(key = "dataverse.pid.ezid.type", value = "ezid")
    @SystemProperty(key = "dataverse.pid.ezid.label", value = "EZID")
    @SystemProperty(key = "dataverse.pid.ezid.authority", value = "10.5072")
    @SystemProperty(key = "dataverse.pid.ezid.ezid.username", value = "user")
    @SystemProperty(key = "dataverse.pid.ezid.ezid.password", value = "pass")
    @SystemProperty(key = "dataverse.ezid.xml.datafile-info", value = "brief")
    void testFactoryCreationWithBriefDatafileInfo() {
        EZIdProviderFactory factory = new EZIdProviderFactory();
        PidProvider provider = factory.createPidProvider("ezid");
        assertNotNull(provider);
        assertTrue(provider instanceof EZIdDOIProvider);
    }

    @Test
    void testXmlGenerationWithDifferentDatafileInfoModes() {
        PidProviderFactoryBean pidService = Mockito.mock(PidProviderFactoryBean.class);
        when(pidService.getProducer()).thenReturn("DANS");

        Dataset dataset = createDatasetWithFiles(
                createDataFile(100L, "text/plain"),
                createDataFile(200L, "application/pdf"),
                createDataFile(300L, "text/plain")
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("datacite.creator", "Author, Test");
        metadata.put("datacite.publicationyear", "2026");

        EZIdDOIProvider expandedProvider = new EZIdDOIProvider("ezid1", "EZID", "10.5072", "FK2/",
                "randomString", SystemConfig.DataFilePIDFormat.DEPENDENT.toString(), "", "",
                "https://ezid.cdlib.org", null, null, "expanded");
        expandedProvider.setPidProviderServiceBean(pidService);

        String expandedXml = expandedProvider.getMetadataFromDvObject("doi:10.5072/FK2/TEST", metadata, dataset, DatafileInfoMode.EXPANDED);
        assertEquals(List.of("100", "200", "300"), XmlPath.from(expandedXml).getList("resource.sizes.size"));
        assertEquals(List.of("text/plain", "application/pdf", "text/plain"), XmlPath.from(expandedXml).getList("resource.formats.format"));

        EZIdDOIProvider briefProvider = new EZIdDOIProvider("ezid2", "EZID", "10.5072", "FK2/",
                "randomString", SystemConfig.DataFilePIDFormat.DEPENDENT.toString(), "", "",
                "https://ezid.cdlib.org", null, null, "brief");
        briefProvider.setPidProviderServiceBean(pidService);

        String briefXml = briefProvider.getMetadataFromDvObject("doi:10.5072/FK2/TEST", metadata, dataset, DatafileInfoMode.BRIEF);
        assertEquals(List.of("600"), XmlPath.from(briefXml).getList("resource.sizes.size"));
        assertEquals(List.of("text/plain", "application/pdf"), XmlPath.from(briefXml).getList("resource.formats.format"));

        EZIdDOIProvider noneProvider = new EZIdDOIProvider("ezid3", "EZID", "10.5072", "FK2/",
                "randomString", SystemConfig.DataFilePIDFormat.DEPENDENT.toString(), "", "",
                "https://ezid.cdlib.org", null, null, "none");
        noneProvider.setPidProviderServiceBean(pidService);

        String noneXml = noneProvider.getMetadataFromDvObject("doi:10.5072/FK2/TEST", metadata, dataset, DatafileInfoMode.NONE);
        assertFalse(noneXml.contains("<sizes>"));
        assertFalse(noneXml.contains("<formats>"));
    }

    private Dataset createDatasetWithFiles(DataFile... dataFiles) {
        Dataset dataset = new Dataset();
        dataset.setGlobalId(new GlobalId("doi", "10.5072", "FK2/FILES", null, null, null));
        dataset.setFiles(List.of(dataFiles));

        DatasetVersion datasetVersion = new DatasetVersion();
        datasetVersion.setVersionState(VersionState.DRAFT);
        datasetVersion.setDataset(dataset);
        datasetVersion.setTermsOfUseAndAccess(new TermsOfUseAndAccess());

        DatasetFieldType titleFieldType = new DatasetFieldType(DatasetFieldConstant.title,
                DatasetFieldType.FieldType.TEXT, false);
        DatasetField titleField = new DatasetField();
        titleField.setDatasetVersion(datasetVersion);
        titleField.setDatasetFieldType(titleFieldType);
        titleField.setSingleValue("First Title");
        datasetVersion.setDatasetFields(List.of(titleField));

        dataset.setVersions(new ArrayList<>(List.of(datasetVersion)));
        DatasetType datasetType = new DatasetType();
        datasetType.setName(DatasetType.DATASET_TYPE_DATASET);
        dataset.setDatasetType(datasetType);
        return dataset;
    }

    private DataFile createDataFile(long fileSize, String contentType) {
        DataFile dataFile = new DataFile();
        dataFile.setFilesize(fileSize);
        dataFile.setContentType(contentType);
        return dataFile;
    }
}
