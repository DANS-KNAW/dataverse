package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.MailUtil;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.mail.internet.InternetAddress;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author Eko Indarto
 */
@ViewScoped
@Named
public class DataverseBridge implements java.io.Serializable {

    private DatasetServiceBean datasetService;
    private DatasetVersionServiceBean datasetVersionService;
    private SettingsServiceBean settingsService;
    private AuthenticationServiceBean authService;
    private MailServiceBean mailServiceBean;
    private DvBridgeConf dvBridgeConf;
    private String userMail;


    private Logger logger = Logger.getLogger(DataverseBridge.class.getCanonicalName());
    private static String RESPONSE_STATE = "{ \"state\":\"value\" }";

    public enum StateEnum {
        IN_PROGRESS("IN-PROGRESS"),
        ERROR("ERROR"),
        FAILED("FAILED"),
        ARCHIVED("ARCHIVED"),
        REJECTED("REJECTED"),
        INVALID("INVALID"),
        DAR_DOWN("DAR-DOWN"),
        BRIDGE_DOWN("BRIDGE-DOWN"),
        INVALID_USER_CREDENTIAL("INVALID_USER_CREDENTIAL"),
        REQUEST_TIME_OUT("REQUEST_TIME_OUT"),
        INTERNAL_SERVER_ERROR("INTERNAL_SERVER_ERROR");
        private String value;

        StateEnum(String value) {
            this.value = value;
        }

        public String toString() {
            return String.valueOf(value);
        }

        public static StateEnum fromValue(String text) {
            for (StateEnum b : StateEnum.values()) {
                if (String.valueOf(b.value).equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }

    public DataverseBridge(String userMail, SettingsServiceBean settingsService, DatasetServiceBean datasetService, DatasetVersionServiceBean datasetVersionService, AuthenticationServiceBean authService, MailServiceBean mailServiceBean){
        this.userMail = userMail;
        this.settingsService = settingsService;
        this.datasetService = datasetService;
        this.datasetVersionService = datasetVersionService;
        this.authService = authService;
        this.mailServiceBean = mailServiceBean;
        this.dvBridgeConf = getDvBridgeConf(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeConf));
    }

    public JsonObject ingestToDar(String ingestData) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(dvBridgeConf.dataverseBridgeUrl + "/archiving");
            logger.finest("json that send to dataverse-bridge server (/archiving):  " + ingestData);
            StringEntity entity = new StringEntity(ingestData);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            CloseableHttpResponse response = httpClient.execute(httpPost);
            switch (response.getStatusLine().getStatusCode()) {
                case HttpStatus.SC_CREATED:
                case HttpStatus.SC_OK:
                    JsonReader readerInprogres = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.IN_PROGRESS.value)));
                    JsonObject jsonObject = readerInprogres.readObject();
                    readerInprogres.close();
                    return jsonObject;
                case HttpStatus.SC_FORBIDDEN:
                    JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.INVALID_USER_CREDENTIAL.value)));
                    JsonObject jo = jsonReader.readObject();
                    jsonReader.close();
                    return jo;
                case HttpStatus.SC_REQUEST_TIMEOUT:
                    JsonReader readerDarDown = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.DAR_DOWN.value)));
                    JsonObject darDownJsonObject = readerDarDown.readObject();
                    readerDarDown.close();;
                    return darDownJsonObject;
            }
        } catch (IOException e) {
            if (e.getMessage().contains("Connection refused")) {
                JsonObject responseJsonObject = reportBridgeDown(e);
                return responseJsonObject;
            }
        }

        JsonReader reader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.INTERNAL_SERVER_ERROR.value)));
        JsonObject responseJsonObject = reader.readObject();
        reader.close();;
        return responseJsonObject;
    }

    public StateEnum checkArchivingProgress(String dvBaseMetadataXml, String persistentId, String datasetVersionFriendlyNumber, String darName) {
        StateEnum state = StateEnum.INTERNAL_SERVER_ERROR;
        logger.info("Check archiving state....");
        JsonObject jsonObjectReponse = null;
        String path = null;
        try {
            path = dvBridgeConf.dataverseBridgeUrl + "/archiving/state?srcMetadataXml="
                    + URLEncoder.encode(dvBaseMetadataXml + persistentId, StandardCharsets.UTF_8.toString())
                    + "&srcMetadataVersion=" + URLEncoder.encode(datasetVersionFriendlyNumber, StandardCharsets.UTF_8.toString()) + "&targetDarName=" + darName;
            jsonObjectReponse = retrieveGETResponseAsJsonObject(path);
            state = StateEnum.fromValue(jsonObjectReponse.getString("state", "UNKNOWN-ERROR"));
            logger.info("ARCHIVING state: " + state);
            if (state == StateEnum.ARCHIVED) {
                logger.info("Update archiving state in the datasetVersion table.");
                updateDatasetVersionToArchived(persistentId, datasetVersionFriendlyNumber, jsonObjectReponse);
            } else
                updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
        } catch (UnsupportedEncodingException e) {
            logger.severe(e.getMessage());
        }
        return state;
    }

    public void updateArchivenoteAndDisplayMessage(String persistentId, String datasetVersionFriendlyNumber, StateEnum state) {
        String msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.transfer");
        String msgDetails = "";
        boolean sendMailToAdmin = true;
        FacesMessage.Severity fs = FacesMessage.SEVERITY_ERROR;
        switch (state) {
            case BRIDGE_DOWN:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.bridgeserver.down");
                break;
            case DAR_DOWN:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.down");
                break;
            case REQUEST_TIME_OUT:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.down");
                break;
            case INVALID_USER_CREDENTIAL:
                sendMailToAdmin = false;
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.credentials");
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.credentials.detail");
                break;
            case FAILED:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.failed");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case REJECTED:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.rejected");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case IN_PROGRESS:
                sendMailToAdmin = false;
                fs = FacesMessage.SEVERITY_INFO;
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress");
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress.detail");
                break;
            case INVALID:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.invalid");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case ERROR:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case INTERNAL_SERVER_ERROR:
                msgDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.unknown");
                break;
        }
        addMessage(fs, msg, msgDetails);
        if (sendMailToAdmin)
            mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail()
                    , state.value, "persistentId: " + persistentId + "\nVersion: " + datasetVersionFriendlyNumber );
    }

    public void updateDataverseVersionState(String persistentId, String datasetVersionFriendlyNumber, String state) {
        Dataset dataset = datasetService.findByGlobalId(persistentId);
        DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
        dv.setArchiveTime(new Date());
        dv.setArchiveNote(state);
        datasetVersionService.update(dv);
    }

    private void updateDatasetVersionToArchived(String persistentId, String datasetVersionFriendlyNumber, JsonObject jsonObjectArchived) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMMM d, yyyy");
        String pid = jsonObjectArchived.getString("pid", "");
        String archiveNoteState = jsonObjectArchived.getString("landingPage", "") + "#" +
                pid +
                "#" + simpleDateFormat.format(new Date());
        logger.info(archiveNoteState);
        updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, archiveNoteState);
        String msg = BundleUtil.getStringFromBundle("dataset.archive.notification.email.archiving.finish.text", Arrays.asList(pid));
        mailServiceBean.sendSystemEmail(userMail, BundleUtil.getStringFromBundle("dataset.archive.notification.email.archiving.finish.subject"), msg);
        logger.info("Mail is send to: " + userMail);
        //todo:send mail to ingester(?)
    }

    private JsonObject retrieveGETResponseAsJsonObject(String path) {
        //see https://stackoverflow.com/questions/21574478/what-is-the-difference-between-closeablehttpclient-and-httpclient-in-apache-http
        try(CloseableHttpClient httpclient = HttpClients.createDefault()){
            HttpGet httpGet = new HttpGet(path);
            httpGet.addHeader("accept", "application/json");
            CloseableHttpResponse response = httpclient.execute(httpGet);
            switch (response.getStatusLine().getStatusCode()) {
                case HttpStatus.SC_OK:
                    JsonReader reader = Json.createReader(new InputStreamReader((response.getEntity().getContent())));
                    JsonObject jsonObject = reader.readObject();
                    reader.close();
                    return jsonObject;
                case HttpStatus.SC_REQUEST_TIMEOUT:
                    JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.DAR_DOWN.value)));
                    JsonObject responseJsonObject = jsonReader.readObject();
                    jsonReader.close();;
                    return responseJsonObject;
            }
        } catch (IOException e) {
            if (e.getMessage().contains("Connection refused")) {
                JsonObject responseJsonObject = reportBridgeDown(e);
                return responseJsonObject;
            }
        }
        JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.INTERNAL_SERVER_ERROR.value)));
        JsonObject responseJsonObject = jsonReader.readObject();
        jsonReader.close();;
        return responseJsonObject;
    }

    private JsonObject reportBridgeDown(IOException e) {
        String systemEmail = settingsService.getValueForKey(SettingsServiceBean.Key.SystemEmail);
        InternetAddress systemAddress = MailUtil.parseSystemAddress(systemEmail);
        mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), "FATAL ERROR ", e.getMessage());
        JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.BRIDGE_DOWN.value)));
        JsonObject responseJsonObject = jsonReader.readObject();
        jsonReader.close();
        return responseJsonObject;
    }

    public void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        if (FacesContext.getCurrentInstance() != null) //we need this check since there is no contex any more when this method is executed by a thread that run in background; eq. Flowable.fromCallable... (DataverseBridgeDialog)
            FacesContext.getCurrentInstance().addMessage(null, message);
    }

    private DvBridgeConf getDvBridgeConf(String dvBridgeSetting) {
        JsonReader jsonReader = Json.createReader(new StringReader(dvBridgeSetting));
        JsonObject dvBridgeSettingJsonObject = jsonReader.readObject();
        jsonReader.close();
        DvBridgeConf dvBridgeConf = new DvBridgeConf(dvBridgeSettingJsonObject.getString("dataverse-bridge-url")
                , dvBridgeSettingJsonObject.getString("user-group")
                , dvBridgeSettingJsonObject.getJsonArray("conf").stream().map(JsonObject.class::cast)
                .collect(Collectors.toMap(
                        k -> k.getJsonString("darName").getString(),
                        v -> v.getString("dvBaseMetadataXml"))));
        return dvBridgeConf;
    }

    public DvBridgeConf getDvBridgeConf() {
        return dvBridgeConf;
    }

    static class DvBridgeConf {
        private String dataverseBridgeUrl;
        private String userGroup;
        private Map<String, String> conf;

        public DvBridgeConf(String dataverseBridgeUrl, String userGroup, Map<String, String> conf) {
            this.dataverseBridgeUrl = dataverseBridgeUrl;
            this.userGroup = userGroup;
            this.conf = conf;
        }

        public String getDataverseBridgeUrl() {
            return dataverseBridgeUrl;
        }

        public String getUserGroup() {
            return userGroup;
        }

        public Map<String, String> getConf() {
            return conf;
        }
    }
}