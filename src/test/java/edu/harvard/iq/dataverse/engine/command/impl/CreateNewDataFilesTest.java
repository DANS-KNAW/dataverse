package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.settings.JvmSettings;
import edu.harvard.iq.dataverse.storageuse.UploadSessionQuotaLimit;
import edu.harvard.iq.dataverse.util.JhoveFileType;
import edu.harvard.iq.dataverse.util.SystemConfig;
import edu.harvard.iq.dataverse.util.testing.JvmSetting;
import edu.harvard.iq.dataverse.util.testing.LocalJvmSettings;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.commons.io.file.FilesUncheck.createDirectories;
import static org.apache.commons.io.file.PathUtils.deleteDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;


@LocalJvmSettings
public class CreateNewDataFilesTest {
    // TODO keep constants for annotations in sync with class name
    Path testDir = Path.of("target/test/").resolve(getClass().getSimpleName());

    @BeforeEach
    public void cleanTmpDir() throws IOException {
        if(testDir.toFile().exists())
            deleteDirectory(testDir);
    }

    @Test
    @JvmSetting(key = JvmSettings.FILES_DIRECTORY, value = "target/test/CreateNewDataFilesTest/tmp")
    public void execute_fails_to_upload_when_tmp_does_not_exist() throws FileNotFoundException {

        mockTmpLookup();
        var cmd = createCmd("scripts/search/data/shape/shapefile.zip", mockDatasetVersion());
        var ctxt = mockCommandContext(mockQuota(true, 0L));

        assertThatThrownBy(() -> cmd.execute(ctxt))
            .isInstanceOf(CommandException.class)
            .hasMessageContaining("Failed to save the upload as a temp file (temp disk space?)")
            .hasRootCauseInstanceOf(NoSuchFileException.class)
            .getRootCause()
            .hasMessageStartingWith("target/test/CreateNewDataFilesTest/tmp/temp/tmp");
    }

    @Test
    @JvmSetting(key = JvmSettings.FILES_DIRECTORY, value = "target/test/CreateNewDataFilesTest/tmp")
    public void execute_fails_to_upload_too_big_files() throws FileNotFoundException {
        createDirectories(Path.of("target/test/CreateNewDataFilesTest/tmp/temp"));

        mockTmpLookup();
        var cmd = createCmd("scripts/search/data/shape/shapefile.zip", mockDatasetVersion());
        var ctxt = mockCommandContext(mockQuota(true, 1000L));

        assertThatThrownBy(() -> cmd.execute(ctxt))
            .isInstanceOf(CommandException.class)
            .hasMessage("This file size (56.0 KB) exceeds the size limit of 1000 B.");
    }

    @Test
    @JvmSetting(key = JvmSettings.FILES_DIRECTORY, value = "target/test/CreateNewDataFilesTest/tmp")
    public void execute_fails_on_too_little_remaining_storage() throws FileNotFoundException {
        createDirectories(Path.of("target/test/CreateNewDataFilesTest/tmp/temp"));

        mockTmpLookup();
        var cmd = createCmd("scripts/search/data/shape/shapefile.zip", mockDatasetVersion());
        var ctxt = mockCommandContext(mockQuota(true, 1000000L));
        try (MockedStatic<JhoveFileType> mockedStatic = Mockito.mockStatic(JhoveFileType.class)) {
            mockedStatic.when(JhoveFileType::getJhoveConfigFile).thenReturn("conf/jhove/jhove.conf");

            assertThatThrownBy(() -> cmd.execute(ctxt))
                .isInstanceOf(CommandException.class)
                .hasMessage("This file (size 56.0 KB) exceeds the remaining storage quota of 500 B.");
        }
    }

    @Test
    @JvmSetting(key = JvmSettings.FILES_DIRECTORY, value = "target/test/CreateNewDataFilesTest/tmp")
    public void execute_succeeds() throws FileNotFoundException, CommandException {
        var tempDir = testDir.resolve("tmp/temp");
        var testFile = "scripts/search/data/binary/trees.zip";
        createDirectories(tempDir);

        mockTmpLookup();
        var cmd = createCmd(testFile, mockDatasetVersion());
        var ctxt = mockCommandContext(mockQuota(false, 1000000L));
        try (MockedStatic<JhoveFileType> mockedStatic = Mockito.mockStatic(JhoveFileType.class)) {
            mockedStatic.when(JhoveFileType::getJhoveConfigFile).thenReturn("conf/jhove/jhove.conf");

            var result = cmd.execute(ctxt);

            assertThat(result.getErrors()).hasSize(0);
            assertThat(result.getDataFiles()).hasSize(1);

            var dataFile = result.getDataFiles().subList(0, 1).get(0);
            var storageId = dataFile.getStorageIdentifier();

            // uploaded zip remains in tmp directory
            assertThat(tempDir.toFile().list()).hasSize(1);
            assertThat(tempDir.resolve(storageId).toFile().length())
                .isEqualTo(new File(testFile).length());
        }
    }

    @Test
    @JvmSetting(key = JvmSettings.FILES_DIRECTORY, value = "target/test/CreateNewDataFilesTest/tmp")
    public void execute_with_2_shapes() throws Exception {
        var tempDir = testDir.resolve("tmp/temp");
        createDirectories(tempDir);

        File testFile = createAndZipFiles(Arrays.asList(
            "shape1.shp", "shape1.shx", "shape1.dbf", "shape1.prj", "shape1.fbn", "shape1.fbx", // 1st shapefile
            "shape2.shp", "shape2.shx", "shape2.dbf", "shape2.prj", // 2nd shapefile
            "shape2.txt", "shape2.pdf", "shape2",                   // single files, same basename as 2nd shapefile
            "README.MD", "shp_dictionary.xls", "notes"              // single files
        ), testDir.resolve("shapes.zip"));

        // TODO mock CDI provider to allow mime type check

        mockTmpLookup();
        var cmd = createCmd(testFile.toString(), mockDatasetVersion());
        var ctxt = mockCommandContext(mockQuota(false, 1000000L));

        try (MockedStatic<JhoveFileType> mockedStatic = Mockito.mockStatic(JhoveFileType.class)) {
            mockedStatic.when(JhoveFileType::getJhoveConfigFile).thenReturn("conf/jhove/jhove.conf");

            // the test
            var result = cmd.execute(ctxt);

            assertThat(result.getErrors()).hasSize(0);
            assertThat(result.getDataFiles()).hasSize(1);

            var dataFile = result.getDataFiles().subList(0, 1).get(0);
            var storageId = dataFile.getStorageIdentifier();

            // uploaded zip remains in tmp directory
            assertThat(tempDir.toFile().list()).hasSize(1);
            assertThat(tempDir.resolve(storageId).toFile().length())
                .isEqualTo(testFile.length());
        }
    }

    // simplified version from ShapefileHandlerTest
    private File createAndZipFiles(List<String> file_names, Path zipfile) throws IOException {
        try (ZipOutputStream zip_stream = new ZipOutputStream(new FileOutputStream(zipfile.toFile()))) {
            for (var file_name : file_names) {
                zip_stream.putNextEntry(new ZipEntry(file_name));
                zip_stream.write("content".getBytes(), 0, 7);
                zip_stream.closeEntry();
            }
        }
        return zipfile.toFile();
    }

    private static @NotNull CreateNewDataFilesCommand createCmd(String name, DatasetVersion dsVersion) throws FileNotFoundException {
        return new CreateNewDataFilesCommand(
            Mockito.mock(DataverseRequest.class),
            dsVersion,
            new FileInputStream(name),
            "example.zip",
            "application/zip",
            null,
            new UploadSessionQuotaLimit(1000L, 500L),
            "sha");
    }

    private static @NotNull CommandContext mockCommandContext(SystemConfig sysCfg) {
        var ctxt = Mockito.mock(CommandContext.class);
        Mockito.when(ctxt.systemConfig()).thenReturn(sysCfg);
        return ctxt;
    }

    private static @NotNull SystemConfig mockQuota(boolean isStorageQuataEnforced, long maxFileUploadSizeForStore) {
        var sysCfg = Mockito.mock(SystemConfig.class);
        Mockito.when(sysCfg.isStorageQuotasEnforced()).thenReturn(isStorageQuataEnforced);
        Mockito.when(sysCfg.getMaxFileUploadSizeForStore(any())).thenReturn(maxFileUploadSizeForStore);
        return sysCfg;
    }

    private static void mockTmpLookup() {
        JvmSettings mockFilesDirectory = Mockito.mock(JvmSettings.class);
        Mockito.when(mockFilesDirectory.lookup()).thenReturn("/mocked/path");
    }

    private static @NotNull DatasetVersion mockDatasetVersion() {
        var dsVersion = Mockito.mock(DatasetVersion.class);
        Mockito.when(dsVersion.getDataset()).thenReturn(Mockito.mock(Dataset.class));
        return dsVersion;
    }

}
