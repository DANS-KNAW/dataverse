package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.impl.CreateNewDataFilesCommand;
import edu.harvard.iq.dataverse.settings.JvmSettings;
import edu.harvard.iq.dataverse.storageuse.UploadSessionQuotaLimit;
import edu.harvard.iq.dataverse.util.JhoveFileType;
import edu.harvard.iq.dataverse.util.SystemConfig;
import edu.harvard.iq.dataverse.util.testing.JvmSetting;
import edu.harvard.iq.dataverse.util.testing.LocalJvmSettings;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static edu.harvard.iq.dataverse.DataFile.ChecksumType.MD5;
import static org.apache.commons.io.file.FilesUncheck.createDirectories;
import static org.apache.commons.io.file.PathUtils.deleteDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;


@LocalJvmSettings
public class CreateNewDataFilesTest {
    // TODO keep constants for annotations in sync with class name
    Path testDir = Path.of("target/test/").resolve(getClass().getSimpleName());
    PrintStream original_stderr;

    @BeforeEach
    public void cleanTmpDir() throws IOException {
        original_stderr = System.err;
        if(testDir.toFile().exists())
            deleteDirectory(testDir);
    }

    @AfterEach void restoreStderr() {
        System.setErr(original_stderr);
    }

    @Test
    @JvmSetting(key = JvmSettings.FILES_DIRECTORY, value = "/tmp/test/CreateNewDataFilesTest/tmp")
    public void zip_performance() throws Exception {
        var tmpUploadStorage = Path.of("/tmp/test/CreateNewDataFilesTest/tmp/temp");
        if(tmpUploadStorage.toFile().exists()) {
            deleteDirectory(tmpUploadStorage);
        }
        createDirectories(tmpUploadStorage); // temp in target would choke intellij

        var random = new SecureRandom();
        var totalNrOfFiles = 0;
        var totalFileSize = 0;
        var nrOfZipFiles = 20;
        var avgNrOfFilesPerZip = 300;
        var avgFileLength = 5000;
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        var tmp = Path.of(Files.createTempDirectory(null).toString());
        var ctxt = mockCommandContext(mockSysConfig(false, 100000000L, MD5, 10000));
        try (var mockedJHoveFileType = Mockito.mockStatic(JhoveFileType.class)) {
            mockedJHoveFileType.when(JhoveFileType::getJhoveConfigFile).thenReturn("conf/jhove/jhove.conf");
            var before = DateTime.now();
            for (var zipNr = 1; zipNr <= nrOfZipFiles; zipNr++) {
                // build the zip
                var zip = tmp.resolve(zipNr + "-data.zip");
                var nrOfFilesInZip = random.nextInt(avgNrOfFilesPerZip * 2);
                try (var zipStream = new ZipOutputStream(new FileOutputStream(zip.toFile()))) {
                    for (var fileInZipNr = 1; fileInZipNr <= nrOfFilesInZip; fileInZipNr++) {
                        // build content for a file
                        var stringLength = random.nextInt(avgFileLength * 2 -5);
                        StringBuilder sb = new StringBuilder(stringLength);
                        for (int i = 0; i < stringLength+10; i++) {// minimal 10 to prevent buffer unerflow
                            sb.append(chars.charAt(random.nextInt(chars.length())));
                        }
                        // add the file to the zip
                        zipStream.putNextEntry(new ZipEntry(fileInZipNr + ".txt"));
                        zipStream.write((sb.toString()).getBytes());
                        zipStream.closeEntry();
                        totalFileSize += stringLength;
                    }
                }

                // upload the zip
                var result = createCmd(zip.toString(), mockDatasetVersion(), 1000L, 500L)
                    .execute(ctxt);
                assertThat(result.getErrors()).hasSize(0);
                assertThat(result.getDataFiles()).hasSize(nrOfFilesInZip);
                totalNrOfFiles += nrOfFilesInZip;
                System.out.println(MessageFormat.format(
                    "Total time: {0}ms; nr of zips {1} total nr of files {2}; total file size {3}",
                    DateTime.now().getMillis() - before.getMillis(), zipNr, totalNrOfFiles, totalFileSize
                ));
            }
            assertThat(tmpUploadStorage.toFile().list()).hasSize(totalNrOfFiles);
        }
    }

    private static @NotNull CreateNewDataFilesCommand createCmd(String name, DatasetVersion dsVersion, long allocatedQuotaLimit, long usedQuotaLimit) throws FileNotFoundException {
        return new CreateNewDataFilesCommand(
            Mockito.mock(DataverseRequest.class),
            dsVersion,
            new FileInputStream(name),
            "example.zip",
            "application/zip",
            null,
            new UploadSessionQuotaLimit(allocatedQuotaLimit, usedQuotaLimit),
            "sha");
    }

    private static @NotNull CommandContext mockCommandContext(SystemConfig sysCfg) {
        var ctxt = Mockito.mock(CommandContext.class);
        Mockito.when(ctxt.systemConfig()).thenReturn(sysCfg);
        return ctxt;
    }

    private static @NotNull SystemConfig mockSysConfig(boolean isStorageQuataEnforced, long maxFileUploadSizeForStore, DataFile.ChecksumType checksumType, int zipUploadFilesLimit) {
        var sysCfg = Mockito.mock(SystemConfig.class);
        Mockito.when(sysCfg.isStorageQuotasEnforced()).thenReturn(isStorageQuataEnforced);
        Mockito.when(sysCfg.getMaxFileUploadSizeForStore(any())).thenReturn(maxFileUploadSizeForStore);
        Mockito.when(sysCfg.getFileFixityChecksumAlgorithm()).thenReturn(checksumType);
        Mockito.when(sysCfg.getZipUploadFilesLimit()).thenReturn(zipUploadFilesLimit);
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
