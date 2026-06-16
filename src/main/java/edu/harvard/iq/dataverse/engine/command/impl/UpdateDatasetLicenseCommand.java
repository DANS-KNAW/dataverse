package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.TermsOfAccess;
import edu.harvard.iq.dataverse.TermsOfUseAndLicense;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.engine.command.*;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.engine.command.exception.InvalidCommandArgumentsException;
import edu.harvard.iq.dataverse.license.License;
import edu.harvard.iq.dataverse.util.BundleUtil;

import java.util.List;

@RequiredPermissions(Permission.EditDataset)
public class UpdateDatasetLicenseCommand extends AbstractDatasetCommand<Dataset> {
    private License license = null;
    private TermsOfAccess customTermsOfAccess = null;
    private TermsOfUseAndLicense customTermsOfUseAndAccess = null;

    public UpdateDatasetLicenseCommand(DataverseRequest aRequest, Dataset dataset, License license) {
        super(aRequest, dataset);
        this.license = license;
    }

    public UpdateDatasetLicenseCommand(DataverseRequest aRequest, Dataset dataset, TermsOfAccess customTermsOfUseAndAccess, TermsOfUseAndLicense customTermsOfUseAndLicense) {
        super(aRequest, dataset);
        this.customTermsOfAccess = customTermsOfUseAndAccess;
        this.customTermsOfUseAndAccess = customTermsOfUseAndLicense;
    }


    @Override
    public Dataset execute(CommandContext ctxt) throws CommandException {
        DatasetVersion datasetVersion = getDataset().getOrCreateEditVersion();
        datasetVersion.setVersionState(DatasetVersion.VersionState.DRAFT);
        Dataset savedDataset = null;

        if (license != null) {
            if (!license.isActive()) {
                throw new InvalidCommandArgumentsException(BundleUtil.getStringFromBundle("updateDatasetLicenseCommand.errors.licenseNotActive", List.of(license.getName())), this);
            }
            TermsOfUseAndLicense termsOfUseAndLicense = datasetVersion.getTermsOfUseAndLicense();
            termsOfUseAndLicense.setLicense(license);

            savedDataset = ctxt.engine().submit(new UpdateDatasetVersionCommand(getDataset(), getRequest()));
        } else if (customTermsOfAccess != null) {
            if (customTermsOfAccess.getTermsOfAccess() == null || customTermsOfUseAndAccess.getTermsOfUse().isBlank()) {
                throw new InvalidCommandArgumentsException(BundleUtil.getStringFromBundle("updateDatasetLicenseCommand.errors.customTermsOfUseNotProvided"), this);
            }
            TermsOfUseAndLicense termsToUpdate = datasetVersion.getTermsOfUseAndLicense();
            applyCustomTerms(termsToUpdate, customTermsOfAccess);
            termsToUpdate.setLicense(null);
            datasetVersion.setTermsOfUseAndLicense(termsToUpdate);
            savedDataset = ctxt.engine().submit(new UpdateDatasetVersionCommand(getDataset(), getRequest()));
        }
        return savedDataset;
    }

    /**
     * Copies all custom term-related fields from the 'source' object
     * to the 'target' object.
     *
     * @param target The TermsOfUseAndAccess object to be modified
     * @param source The TermsOfUseAndAccess object containing the new data
     */
    private void applyCustomTerms(TermsOfUseAndAccess target, TermsOfUseAndAccess source) {
        target.setTermsOfUse(source.getTermsOfUse());
        target.setConfidentialityDeclaration(source.getConfidentialityDeclaration());
        target.setSpecialPermissions(source.getSpecialPermissions());
        target.setRestrictions(source.getRestrictions());
        target.setCitationRequirements(source.getCitationRequirements());
        target.setDepositorRequirements(source.getDepositorRequirements());
        target.setConditions(source.getConditions());
        target.setDisclaimer(source.getDisclaimer());
    }
}
