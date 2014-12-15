package gr.iti.mklab.focused.crawler.filters;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import gr.iti.mklab.framework.client.mongo.DAOFactory;
import gr.iti.mklab.framework.common.domain.Account;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Item;

public class MentionsItemFilter extends ItemFilter {

	private List<String> ids;
	private String listId;

	public MentionsItemFilter(Configuration configuration) {
		super(configuration);
		try {
		this.listId =configuration.getParameter("listId");
		
		String host =configuration.getParameter("host");
		String database =configuration.getParameter("database");
		
		BasicDAO<Account, String> dao = new DAOFactory().getDAO(host, database, Account.class);
			
		Query<Account> q = dao.getDatastore().createQuery(Account.class);
		q.filter("listId", listId);
    	
		List<Account> accounts = dao.find(q).asList();
		ids = new ArrayList<String>();
		for(Account account : accounts) {
			ids.add(account.getSource() + "#" + account.getId());
		}
		
		Logger.getLogger(MentionsItemFilter.class).info("Initialized. " + 
				ids.size() + " ids from list " + listId + " to be used in mentions filter");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean accept(Item item) {
		
		String[] mentions = item.getMentions();
		if(mentions != null && mentions.length > 2) {
			incrementDiscarded();
			return false;
		}
		
		String[] lists = item.getList();
		if(lists == null || lists.length==0 || lists.length>1) {
			incrementAccepted();
			return true;
		}
		
		if(!lists[0].equals(listId)) {
			incrementAccepted();
			return true;
		}
		
		String uid = item.getUserId();
		if (!ids.contains(uid)) {
			incrementDiscarded();
			return false;
		}
		
		return true;
	}

	@Override
	public String name() {
		return "MentionsItemFilter";
	}
	
}