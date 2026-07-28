/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.util.BundleUtil;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

/**
 *
 * @author skraffmi
 */
public class TermsOfUseOrLicenseValidator implements ConstraintValidator<ValidateTermsOfUseAndAccess, TermsOfUseOrLicense> {

    @Override
    public void initialize(ValidateTermsOfUseAndAccess constraintAnnotation) {

    }

    @Override
    public boolean isValid(TermsOfUseOrLicense value, ConstraintValidatorContext context) {

        return isTOUAValid(value, context);

    }

    public static boolean isTOUAValid(TermsOfUseOrLicense value, ConstraintValidatorContext context){

        //if part of a template it is valid
        if (value.getTemplate() != null){
            return true;
        }

         //If there are no restricted files then terms are valid
        if (!value.getDatasetVersion().isHasRestrictedFile()) {
            return true;
        }
        /*If there are restricted files then the version
        must allow access requests or have terms of use filled in.
         */
        boolean valid = (value.getTermsOfUse() != null && !value.getTermsOfUse().isEmpty());
        if (!valid) {
            try {
                if (context != null) {
                    context.buildConstraintViolationWithTemplate(BundleUtil.getStringFromBundle("dataset.message.toua.invalid")).addConstraintViolation();
                }

               value.setValidationMessage(BundleUtil.getStringFromBundle("dataset.message.toua.invalid"));
            } catch (NullPointerException e) {
                return false;
            }
            return false;
        }
        return valid;
    }
}


