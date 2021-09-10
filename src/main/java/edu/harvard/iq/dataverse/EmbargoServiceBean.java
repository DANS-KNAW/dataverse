package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.actionlogging.ActionLogRecord;
import edu.harvard.iq.dataverse.actionlogging.ActionLogServiceBean;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.util.List;

/**
 * @author mderuijter
 */
@Stateless
@Named
public class EmbargoServiceBean {

    @PersistenceContext
    EntityManager em;

    @EJB
    ActionLogServiceBean actionLogSvc;

    public List<Embargo> findAllEmbargoes() {
        return em.createNamedQuery("Embargo.findAll", Embargo.class).getResultList();
    }

    public Embargo findByEmbargoId(Long id) {
        Query query = em.createNamedQuery("Embargo.findById", Embargo.class);
        query.setParameter("id", id);
        try {
            return (Embargo) query.getSingleResult();
        } catch (Exception ex) {
            return null;
        }
    }

    public Long save(Embargo embargo) {
        if (embargo.getId() == null) {
            em.persist(embargo);
            em.flush();
            actionLogSvc.log(new ActionLogRecord(ActionLogRecord.ActionType.Admin, "embargoCreate")
                    .setInfo("date available: " + embargo.getDateAvailable() + " reason: " + embargo.getReason()));
        }
        return embargo.getId();
    }

    public int deleteById(long id) {
        actionLogSvc.log(new ActionLogRecord(ActionLogRecord.ActionType.Admin, "embargoDelete")
                .setInfo(Long.toString(id)));
        return em.createNamedQuery("Embargo.deleteById")
                .setParameter("id", id)
                .executeUpdate();
    }
}
