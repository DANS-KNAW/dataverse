/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.dataverse;

import jakarta.validation.ConstraintValidatorContext;

/**
 * Utility class for validating TermsOfUseAndAccess.
 * Delegates to appropriate validator based on object type.
 *
 * @author skraffmi
 */
public class TermsOfUseAndAccessValidator {

    /**
         * Validates a TermsOfAccess or TermsOfUseOrLicense object.
         * @param value the object to validate (TermsOfAccess or TermsOfUseOrLicense)
     * @param context the constraint validator context
     * @return true if valid, false otherwise
     */
    public static boolean isTOUAValid(Object value, ConstraintValidatorContext context){

        if (value instanceof TermsOfAccess) {
            return TermsOfAccessValidator.isTOUAValid((TermsOfAccess) value, context);
        } else if (value instanceof TermsOfUseOrLicense) {
            return TermsOfUseOrLicenseValidator.isTOUAValid((TermsOfUseOrLicense) value, context);
        } else {
            return true;
        }
    }
}
