package edu.harvard.iq.dataverse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.authorization.users.ApiToken;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
    DataverseSession session;
    @EJB
    AuthenticationServiceBean authService;
    @EJB
    MailServiceBean mailServiceBean;


    private Logger logger = Logger.getLogger(DataverseBridgeDialog.class.getCanonicalName());
    private String datasetVersionFriendlyNumber;
    private String darUsername;
    private String darPassword;
    private Map<String, String> dvDarConfs = new HashMap<String, String>();
    private String darName = "EASY";
    private List<String> darNames;
    private String persistentId;
    private DataverseBridge dataverseBridge;

    @PostConstruct
    public void init() {
        if (session.getUser().isAuthenticated() && settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeConf) != null) {
            dataverseBridge =  new DataverseBridge(((AuthenticatedUser) session.getUser()).getEmail(), settingsService, datasetService, datasetVersionService, authService, mailServiceBean);
            dvDarConfs = dataverseBridge.getDvBridgeConf().getConf();
            darNames = dvDarConfs.keySet().stream().collect(Collectors.toList());
        }
    }

    public void reload(String path) throws IOException {
        ExternalContext ec = FacesContext.getCurrentInstance().getExternalContext();
        ec.redirect(path);
    }

    public void archive() {
        logger.info("INGEST TO Digital Archive Repository");

        AuthenticatedUser au = (AuthenticatedUser) session.getUser();
        logger.info(" The user '" + au.getIdentifier() + "' is trying to archive '" + persistentId + "' to '" + darName + "'.");
        DataverseBridge.StateEnum state;
        String dvBaseMetadataXml = dvDarConfs.get(darName);
        try {
            JsonObject postResponseJsonObject = dataverseBridge.ingestToDar(composeJsonIngestData(dvBaseMetadataXml, darName));
            state = DataverseBridge.StateEnum.fromValue(postResponseJsonObject.getString("state"));
        } catch (IOException e) {
            logger.severe(e.getMessage());
            state = DataverseBridge.StateEnum.INTERNAL_SERVER_ERROR;
        }
        if (state == DataverseBridge.StateEnum.IN_PROGRESS) {
            dataverseBridge.updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.toString() + "@" + darName);
            Flowable.fromCallable(() -> {
                DataverseBridge.StateEnum currentState = DataverseBridge.StateEnum.IN_PROGRESS;
                int hopCount = 0;
                while ((currentState == DataverseBridge.StateEnum.IN_PROGRESS) && hopCount < 10) {
                    Thread.sleep(900000);//15 minutes
                    hopCount += 1;
                    logger.info(".... Checking Archiving Progress of " + persistentId + ".....[" + hopCount + "]");
                    currentState = dataverseBridge.checkArchivingProgress(dvBaseMetadataXml, persistentId, datasetVersionFriendlyNumber, darName);
                }
                logger.info("Hop count: " + hopCount + "\t Current state of '" + persistentId + "': " + currentState);
                return currentState;
            })
                    .subscribeOn(Schedulers.io())
                    .observeOn(Schedulers.single())
                    .doOnError(
                            throwable -> {
                                String es = throwable.toString();
                                logger.severe(es);
                            }
                    )
                    .subscribe(cs -> logger.info("The dataset of '" + persistentId + "' has been archived."),
                            throwable -> logger.severe(throwable.getCause().getMessage()));

            FacesMessage message = new FacesMessage(FacesMessage.SEVERITY_INFO
                    , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress")
                    , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress.detail"));
            FacesContext.getCurrentInstance().addMessage(null, message);
        } else {
            dataverseBridge.updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
        }

    }

    private String composeJsonIngestData(String dvBaseMetadataXml, String darName) throws JsonProcessingException {
        SrcData srcData = new SrcData(dvBaseMetadataXml + persistentId, datasetVersionFriendlyNumber, getApiTokenKey());
        DarData darData = new DarData(darName, darUsername, darPassword);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(new IngestData(srcData, darData));
    }

    private String getApiTokenKey() {
        //No need user auth check since this class will be only accessed by auth user.
        ApiToken apiToken;
        AuthenticatedUser au = (AuthenticatedUser) session.getUser();
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
        return "";
    }


    public String getDarUsername() {
        return darUsername;
    }

    public void setDarUsername(String darUsername) {
        this.darUsername = darUsername;
    }

    public String getDarPassword() {
        return darPassword;
    }

    public void setDarPassword(String darPassword) {
        this.darPassword = darPassword;
    }


    public void setDatasetVersionFriendlyNumber(String datasetVersionFriendlyNumber) {
        this.datasetVersionFriendlyNumber = datasetVersionFriendlyNumber;
    }

    public String getPersistentId() {
        return persistentId;
    }

    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }


    public void setDarNames(Map<String, String> dvDarConfs) {
        this.dvDarConfs = dvDarConfs;
    }

    public String getDarName() {
        return darName;
    }

    public void setDarName(String darName) {
        this.darName = darName;
    }

    public List<String> getDarNames() {
        return darNames;
    }



    private class SrcData {
        private String srcXml;
        private String srcVersion;
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

        public String getApiToken() {
            return apiToken;
        }
    }
    private class DarData {
        private String username;
        private String password;
        private String darName;
        public DarData(String darName, String username, String password) {
            this.darName = darName;
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDarName() {
            return darName;
        }
    }
    private class IngestData{
        private SrcData srcData;
        private DarData darData;

        public IngestData(SrcData srcData, DarData darData) {
            this.srcData = srcData;
            this.darData = darData;
        }

        public SrcData getSrcData() {
            return srcData;
        }

        public DarData getDarData() {
            return darData;
        }
    }

}