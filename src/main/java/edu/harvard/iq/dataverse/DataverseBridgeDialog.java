package edu.harvard.iq.dataverse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.authorization.users.ApiToken;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.schedulers.Schedulers;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import javax.faces.view.ViewScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

/**
 *
 * @author Eko Indarto
 */
@ViewScoped
@Named
public class DataverseBridgeDialog implements java.io.Serializable {

    @EJB
    DatasetServiceBean datasetService;
    @EJB
    DatasetVersionServiceBean datasetVersionService;
    @EJB
    SettingsServiceBean settingsService;
    @EJB
    DataverseServiceBean dataverseService;
    @Inject
    DataverseSession dataverseSession;
    @EJB
    AuthenticationServiceBean authService;

    private Logger logger = Logger.getLogger(DataverseBridgeDialog.class.getCanonicalName());
    private String datasetVersionFriendlyNumber;
    private String tdrUsername;
    private String tdrPassword;
    private String persistentId;

    private boolean dataverseBridgeEnabled;

    private static String STATE_ARCHIVED = "ARCHIVED";
    private static String STATE_INPROGRESS = "IN-PROGRESS";
    private static String STATE_FAILED = "FAILED";

    @PostConstruct
    public void init() {
        if (dataverseSession.getUser().isAuthenticated()) {
            dataverseBridgeEnabled = settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) != null
                                        && settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) != null
                                        && settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) != null;

        }
    }

    public void reload(String path) throws IOException {
        ExternalContext ec = FacesContext.getCurrentInstance().getExternalContext();
        ec.redirect(path);
    }

    public boolean isDataverseBridgeEnabled() {
        return dataverseBridgeEnabled;
    }

    public void ingestToTdr() {
        logger.info("INGEST TO TDR");
        try {
            JsonObject jsonObjectIngestResponse = getJsonObjectOfPostResponse("/archive/create");
            if (jsonObjectIngestResponse != null) {
                Dataset dataset = datasetService.findByGlobalId(persistentId);
                DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
                dv.setArchiveNote(STATE_INPROGRESS);
                datasetVersionService.update(dv);
                checkArchivingProgress();
            }

        } catch (IOException e) {
            logger.severe(e.getMessage());
            if (e.getMessage().contains("Connection refused"))
                addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.bridgeserver.down"), null);

        }

        logger.info("Archive is in process..... Please Refresh it.");
    }

    private void checkArchivingProgress() {
        final ArchivingProgressState archivingProgressState = new ArchivingProgressState();
        Flowable.fromCallable(() -> {
            String state = STATE_INPROGRESS;
            while (state.equals(STATE_INPROGRESS)) {
                logger.info("Check archiving state....");
                JsonObject jsonObjectArchived = getJsonObjectOfGetResponse(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeUrl, "") + "/archive/state?srcXml="
                        + URLEncoder.encode(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) + persistentId, StandardCharsets.UTF_8.toString())
                        + "&srcVersion=" + URLEncoder.encode(datasetVersionFriendlyNumber, StandardCharsets.UTF_8.toString()) + "&targetIri=" + URLEncoder.encode(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeTdrIri), StandardCharsets.UTF_8.toString()));
                if(jsonObjectArchived == null) {
                    state = STATE_FAILED;
                    archivingProgressState.setFinish(true);
                    updateDataverseVersionToFailed();
                    logger.severe("Archiving is failed.");
                    break;
                }

                state = jsonObjectArchived.getString("state", "");
                if (state.equals(STATE_INPROGRESS))
                    Thread.sleep(15000);
                else {
                    archivingProgressState.setFinish(true);
                    if (state.equals(STATE_ARCHIVED)) {
                        logger.info("Update archiving state in the datasetVersion table.");
                        updateDatasetVersionToArchived(jsonObjectArchived);
                        //The following code is commented out since it will not work: FacesMessage inside a new threads
                        //addMessage(FacesMessage.SEVERITY_INFO,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.success.archived"), null);
                    } else {
                        logger.warning("ARCHIVING FAILED");
                        updateDataverseVersionToFailed();
                        //The following code is commented out since it will not work: FacesMessage inside a new threads
                        //addMessage(FacesMessage.SEVERITY_INFO,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.failed.archived"), null);
                    }
                }
            }
            return archivingProgressState;
        })
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.single())
                .doOnComplete(new Action() {
                    @Override
                    public void run()  {
                            if (archivingProgressState.isFinish()){
                                //check state, it can be failed or archived
                                //send mail to ingester
                                logger.info("Archiving finish.");
                            }
                    }
                })
                .subscribe();
    }

    private void updateDataverseVersionToFailed() {
        Dataset dataset = datasetService.findByGlobalId(persistentId);
        DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
        dv.setArchiveNote(STATE_FAILED);
        datasetVersionService.update(dv);
    }

    private void updateDatasetVersionToArchived(JsonObject jsonObjectArchived) {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMMM d, yyyy");
        String archiveNote = "<a href=\"" + jsonObjectArchived.getString("landingPage", "") + "\">" +
                                jsonObjectArchived.getString("doi", "") +
                             "<a>" + "<br/>" + simpleDateFormat.format(date);
        logger.info(archiveNote);
        Dataset dataset = datasetService.findByGlobalId(persistentId);
        DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
        dv.setArchiveTime(date);
        dv.setArchiveNote(archiveNote);
        datasetVersionService.update(dv);
    }

    private JsonObject getJsonObjectOfGetResponse(String path) throws Exception {
        JsonObject jsonObject = null;
        JsonReader reader = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(path);
        httpGet.addHeader("accept", "application/json");
        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                reader = Json.createReader(new InputStreamReader((response.getEntity().getContent())));
                jsonObject = reader.readObject();
                reader.close();
            }
        }
        finally {
            if (reader != null) reader.close();
            if (httpClient != null) httpClient.close();
            if (response != null) response.close();
        }
        return jsonObject;
    }

    private JsonObject getJsonObjectOfPostResponse(String path) throws IOException {
        JsonObject jsonObject = null;
        CloseableHttpResponse httpResponse = getCloseableHttpResponse(path);
        switch (httpResponse.getStatusLine().getStatusCode()) {
            case HttpStatus.SC_CREATED: JsonReader reader = Json.createReader(new StringReader(readEntityAsString(httpResponse.getEntity())));
                                        jsonObject = reader.readObject();
                                        reader.close();
                                        break;

            case HttpStatus.SC_FORBIDDEN: addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdrcredentias"), null);
                                        break;

            case HttpStatus.SC_REQUEST_TIMEOUT: addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.request.timed.out"), null);
                                        break;

            default:addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.default"), null);

        }
        return jsonObject;

    }

    private CloseableHttpResponse getCloseableHttpResponse(String path) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeUrl, "") + path);
        String jsonIngestData = composeJsonIngestData();
        logger.finest("json that send to dataverse-bridge server (/archive/create):  " + jsonIngestData);
        StringEntity entity = new StringEntity(jsonIngestData);
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");

        return httpClient.execute(httpPost);
    }

    public void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        FacesContext.getCurrentInstance().addMessage(null, message);
    }

    private String composeJsonIngestData() throws JsonProcessingException {
        SrcData srcData = new SrcData(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) + persistentId, datasetVersionFriendlyNumber, getApiTokenKey());

        TdrData tdrData = new TdrData(tdrUsername, tdrPassword, settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeTdrIri));
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(new IngestData(srcData, tdrData));
    }

    private String readEntityAsString(HttpEntity entity) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(entity.getContent(), bos);
        return new String(bos.toByteArray(), "UTF-8");
    }

    /* THIS IS TworRavensHelper Code, we should make it public on th TwoRavensHelper class and use it here*/
    private String getApiTokenKey() {
        ApiToken apiToken;
        if (dataverseSession.getUser() == null) {
            return null;
        }
        if (dataverseSession.getUser().isAuthenticated()) {
            AuthenticatedUser au = (AuthenticatedUser) dataverseSession.getUser();
            apiToken = authService.findApiTokenByUser(au);
            if (apiToken != null) {
                return apiToken.getTokenString();
            }
            // Generate if not available?
            // Or should it just be generated inside the authService
            // automatically?
            apiToken = authService.generateApiTokenForUser(au);
            if (apiToken != null) {
                return apiToken.getTokenString();
            }
        }
        return "";
    }


    public String getTdrUsername() {
        return tdrUsername;
    }

    public void setTdrUsername(String tdrUsername) {
        this.tdrUsername = tdrUsername;
    }

    public String getTdrPassword() {
        return tdrPassword;
    }

    public void setTdrPassword(String tdrPassword) {
        this.tdrPassword = tdrPassword;
    }


    public void setDatasetVersionFriendlyNumber(String datasetVersionFriendlyNumber) {
        this.datasetVersionFriendlyNumber = datasetVersionFriendlyNumber;
    }


    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }

    private class SrcData {
        private String srcXml;
        private String srcVersion;
        private String appName = "DATAVERSE";
        private String apiToken;

        public SrcData(String srcXml, String srcVersion, String apiToken) {
            this.srcXml = srcXml;
            this.srcVersion = srcVersion;
            this.apiToken = apiToken;
        }

        public String getSrcXml() {
            return srcXml;
        }

        public String getSrcVersion() {
            return srcVersion;
        }

        public String getAppName() {
            return appName;
        }

        public String getApiToken() {
            return apiToken;
        }
    }
    private class TdrData {
        private String username;
        private String password;
        private String iri;
        private String appName = "EASY";

        public TdrData(String username, String password, String iri) {
            this.username = username;
            this.password = password;
            this.iri = iri;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getIri() {
            return iri;
        }

        public String getAppName() {
            return appName;
        }
    }
    private class IngestData{
        private SrcData srcData;
        private TdrData tdrData;

        public IngestData(SrcData srcData, TdrData tdrData) {
            this.srcData = srcData;
            this.tdrData = tdrData;
        }

        public SrcData getSrcData() {
            return srcData;
        }

        public TdrData getTdrData() {
            return tdrData;
        }
    }

    private class ArchivingProgressState {
        private boolean finish;

        public boolean isFinish() {
            return finish;
        }

        public void setFinish(boolean finish) {
            this.finish = finish;
        }
    }

}