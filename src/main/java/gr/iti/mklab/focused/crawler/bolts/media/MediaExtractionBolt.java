package gr.iti.mklab.focused.crawler.bolts.media;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.WebPage;
import gr.iti.mklab.framework.retrievers.Retriever;
import gr.iti.mklab.framework.retrievers.impl.DailyMotionRetriever;
import gr.iti.mklab.framework.retrievers.impl.FacebookRetriever;
import gr.iti.mklab.framework.retrievers.impl.FlickrRetriever;
import gr.iti.mklab.framework.retrievers.impl.InstagramRetriever;
import gr.iti.mklab.framework.retrievers.impl.TwitpicRetriever;
import gr.iti.mklab.framework.retrievers.impl.VimeoRetriever;
import gr.iti.mklab.framework.retrievers.impl.YoutubeRetriever;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class MediaExtractionBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger _logger;
	
	private OutputCollector _collector;
	
	private static Pattern instagramPattern 	= 	Pattern.compile("https*://instagram.com/p/([\\w\\-]+)/");
	private static Pattern youtubePattern 		= 	Pattern.compile("https*://www.youtube.com/watch?.*v=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
	private static Pattern vimeoPattern 		= 	Pattern.compile("https*://vimeo.com/([0-9]+)/*$");
	private static Pattern twitpicPattern 		= 	Pattern.compile("https*://twitpic.com/([A-Za-z0-9]+)/*.*$");
	private static Pattern dailymotionPattern 	= 	Pattern.compile("https*://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
	private static Pattern facebookPattern 		= 	Pattern.compile("https*://www.facebook.com/photo.php?.*fbid=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
	private static Pattern flickrPattern 		= 	Pattern.compile("https*://flickr.com/photos/([A-Za-z0-9@]+)/([A-Za-z0-9@]+)/*.*$");
	
	private Map<String, Retriever> retrievers = new HashMap<String, Retriever>();
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("MediaItem"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		_collector = collector;  		
		_logger = Logger.getLogger(MediaExtractionBolt.class);
		
		Credentials instaCredentials = new Credentials();
		instaCredentials.setClientId("");
		instaCredentials.setKey("");
		retrievers.put("instagram", new InstagramRetriever(instaCredentials));
		
		Credentials yrCredentials = new Credentials();
		yrCredentials.setClientId("");
		yrCredentials.setKey("");
		retrievers.put("youtube", new YoutubeRetriever(yrCredentials));
		
		Credentials vimeoCredentials = new Credentials();
		vimeoCredentials.setClientId("");
		vimeoCredentials.setKey("");
		retrievers.put("vimeo", new VimeoRetriever(vimeoCredentials));
		
		Credentials twpCredentials = new Credentials();
		twpCredentials.setClientId("");
		twpCredentials.setKey("");
		retrievers.put("twitpic", new TwitpicRetriever(twpCredentials));
		
		Credentials dmCredentials = new Credentials();
		dmCredentials.setClientId("");
		dmCredentials.setKey("");
		retrievers.put("dailymotion", new DailyMotionRetriever(dmCredentials));
		
		Credentials fbCredentials = new Credentials();
		fbCredentials.setAccessToken("");
		retrievers.put("facebook", new FacebookRetriever(fbCredentials));
		
		Credentials flickrCredentials = new Credentials();
		flickrCredentials.setKey("");
		flickrCredentials.setSecret("");
		retrievers.put("flickr", new FlickrRetriever(flickrCredentials));
		
	}

	public void execute(Tuple input) {
		
		WebPage webPage = (WebPage) input.getValueByField("webpages");
		if(webPage == null) {
			return;
		}
		
		String expandedUrl = webPage.getExpandedUrl();
		_logger.info(expandedUrl);
		
		try {
			MediaItem mediaItem = getMediaItem(expandedUrl);	
			
			if(mediaItem != null) {
				webPage.setMedia(1);
				String[] mediaIds = {mediaItem.getId()};
				webPage.setMediaIds(mediaIds);
				mediaItem.setReference(webPage.getReference());
				
				_collector.emit(tuple(mediaItem));	
				_collector.ack(input);		
			}
			else {
				_logger.error(webPage.getExpandedUrl() + " failed due to null media item");
				_collector.fail(input);	
			}
			
		} catch (Exception e) {
			_logger.error(webPage.getExpandedUrl() + " failed due to exception: " + e.getMessage());
			_collector.ack(input);	
		}
		
	}   
	
	private MediaItem getMediaItem(String url) {
		Retriever retriever = null;
		String mediaId = null;
		String source = null;
		
		Matcher matcher;
		if((matcher = instagramPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("instagram");
			source = "instagram";
		}
		else if((matcher = youtubePattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("youtube");
			source = "youtube";
		}
		else if((matcher = vimeoPattern.matcher(url)).matches()){
			mediaId = matcher.group(1);
			retriever = retrievers.get("vimeo");
			source = "vimeo";
		}
		else if((matcher = twitpicPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("twitpic");
			source = "twitpic";
		}
		else if((matcher = dailymotionPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("dailymotion");
			source = "dailymotion";
		}
		else if((matcher = facebookPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("facebook");
			source = "facebook";
		}
		else if((matcher = flickrPattern.matcher(url)).matches()) {
			mediaId = matcher.group(2);
			retriever = retrievers.get("flickr");
			source = "flickr";
		}
		else {
			_logger.error(url + " matches nothing.");
			return null;
		}
		
		if(mediaId == null || retriever == null) {
			return null;
		}
		
		try {
			MediaItem mediaItem = retriever.getMediaItem(mediaId);
			if(mediaItem == null) {
				_logger.info(mediaId + " from " + source + " is null");
				return null;
			}
			
			mediaItem.setPageUrl(url);
			
			StreamUser streamUser = mediaItem.getUser();
			String userid = mediaItem.getUserId();
			if(streamUser == null || userid == null) {
				streamUser = retriever.getStreamUser(userid);
				if(streamUser == null) {
					throw new Exception("Missing " + mediaItem.getSource() + " user: " + userid);
				}
				mediaItem.setUser(streamUser);
				mediaItem.setUserId(streamUser.getId());
			}
			
			return mediaItem;
		}
		catch(Exception e) {
			_logger.error(e);
			return null;
		}
	}

	@Override
	public void cleanup() {
		
	}
}