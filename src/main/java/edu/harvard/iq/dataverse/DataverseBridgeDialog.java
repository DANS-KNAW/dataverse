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

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.*;
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
    DataverseSession dataverseSession;
    @EJB
    AuthenticationServiceBean authService;
    @EJB
    MailServiceBean mailServiceBean;


    private Logger logger = Logger.getLogger(DataverseBridgeDialog.class.getCanonicalName());
    private String datasetVersionFriendlyNumber;
    private String tdrUsername;
    private String tdrPassword;
    private Map<String, String> dvTdrConfs = new HashMap<String, String>();
    private String tdrName = "EASY";
    private List<String> tdrNames;
    private String persistentId;
    private DataverseBridge dataverseBridge;

    @PostConstruct
    public void init() {
        if (dataverseSession.getUser().isAuthenticated() && settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeConf) != null) {
            dataverseBridge =  new DataverseBridge(settingsService, datasetService, datasetVersionService, authService, mailServiceBean);
            dvTdrConfs = dataverseBridge.getDvBridgeConf().getConf();
            tdrNames = dvTdrConfs.keySet().stream().collect(Collectors.toList());
        }
    }

    public void reload(String path) throws IOException {
        ExternalContext ec = FacesContext.getCurrentInstance().getExternalContext();
        ec.redirect(path);
    }

    public void archive() {
        logger.info("INGEST TO TDR");

        if (tdrName.equals("EASY") || tdrName.equals("DATAVERSE")) {
            DataverseBridge.StateEnum state;
            String dvBaseMetadataXml = dvTdrConfs.get(tdrName);
            try {
                state = dataverseBridge.ingestToTdr(composeJsonIngestData(dvBaseMetadataXml,tdrName));
            } catch (IOException e) {
                logger.severe(e.getMessage());
                state = DataverseBridge.StateEnum.INTERNAL_SERVER_ERROR;
            }
            if (state == DataverseBridge.StateEnum.IN_PROGRESS) {
                dataverseBridge.updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.toString() + tdrName);
                Flowable.fromCallable(() -> {
                    DataverseBridge.StateEnum currentState = DataverseBridge.StateEnum.IN_PROGRESS;
                    int hopCount = 0;
                    while (currentState == DataverseBridge.StateEnum.IN_PROGRESS || hopCount == 10) {
                        Thread.sleep(600000);//10 minutes
                        hopCount += 1;
                        logger.info(".... Cheking Archiving Progress .....[" + hopCount + "]");
                        currentState = dataverseBridge.checkArchivingProgress(dvBaseMetadataXml, persistentId, datasetVersionFriendlyNumber, tdrName);
                    }
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
                        .doOnComplete(new Action() {
                            @Override
                            public void run() {
                                // if (archivingProgressState.isFinish()){
                                //check state, it can be failed or archived
                                //todo: send mail to ingester
                                logger.info("===== Archiving finish ======");
                                //}
                            }
                        })
                        .subscribe();
                addMessage(FacesMessage.SEVERITY_INFO, BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress"), null);
            } else {
                dataverseBridge.updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
            }
        } else
            addMessage(FacesMessage.SEVERITY_INFO, "'" + tdrName + " repository is currently unavailable." , "Please use 'EASY repository'");
    }

    public void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        FacesContext.getCurrentInstance().addMessage(null, message);
    }

    private String composeJsonIngestData(String dvBaseMetadataXml, String tdrName) throws JsonProcessingException {
        SrcData srcData = new SrcData(dvBaseMetadataXml + persistentId, datasetVersionFriendlyNumber, getApiTokenKey());

        TdrData tdrData = new TdrData(tdrName, tdrUsername, tdrPassword);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(new IngestData(srcData, tdrData));
    }

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

    public String getPersistentId() {
        return persistentId;
    }

    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }


    public void setTdrNames(Map<String, String> dvTdrConfs) {
        this.dvTdrConfs = dvTdrConfs;
    }

    public String getTdrName() {
        return tdrName;
    }

    public void setTdrName(String tdrName) {
        this.tdrName = tdrName;
    }

    public List<String> getTdrNames() {
        return tdrNames;
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
    private class TdrData {
        private String username;
        private String password;
        private String tdrName;
        public TdrData(String tdrName, String username, String password) {
            this.tdrName = tdrName;
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getTdrName() {
            return tdrName;
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

}