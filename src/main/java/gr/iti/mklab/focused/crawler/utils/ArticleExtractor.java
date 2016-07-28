package gr.iti.mklab.focused.crawler.utils;

import gr.iti.mklab.framework.common.domain.Article;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.WebPage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.InputSource;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.Image;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.document.TextDocumentStatistics;
import de.l3s.boilerpipe.estimators.SimpleEstimator;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.ImageExtractor;

public class ArticleExtractor {

	private static Logger _logger = Logger.getLogger(ArticleExtractor.class);
	
	private static int minDim = 200;
	private static int minArea = 200 * 200;
	private static int maxUrlLength = 500;
	
	private static BoilerpipeExtractor _extractor = CommonExtractors.ARTICLE_EXTRACTOR;
	private static BoilerpipeExtractor _articleExtractor = CommonExtractors.ARTICLE_EXTRACTOR;
	private static ImageExtractor _imageExtractor = ImageExtractor.INSTANCE;
	private static SimpleEstimator _estimator = SimpleEstimator.INSTANCE;	;
	
	public static boolean parseWebPage(WebPage webPage, byte[] content, List<MediaItem> mediaItems) {  
	  	try { 
	  		String base = webPage.getExpandedUrl();
	  		
	  		InputSource articelIS1 = new InputSource(new ByteArrayInputStream(content));
	  		InputSource articelIS2 = new InputSource(new ByteArrayInputStream(content));
	  		
		  	TextDocument document = null, imgDoc = null;

	  		document = new BoilerpipeSAXInput(articelIS1).getTextDocument();
	  		imgDoc = new BoilerpipeSAXInput(articelIS2).getTextDocument();
	  		
	  		TextDocumentStatistics dsBefore = new TextDocumentStatistics(document, false);
	  		synchronized(_articleExtractor) {
	  			_articleExtractor.process(document);
	  		}
	  		synchronized(_extractor) {
	  			_extractor.process(imgDoc);
	  		}
	  		TextDocumentStatistics dsAfter = new TextDocumentStatistics(document, false);
	  		
	  		boolean isLowQuality = true;
	  		synchronized(_estimator) {
	  			isLowQuality = _estimator.isLowQuality(dsBefore, dsAfter);
	  		}
	  	
	  		String title = document.getTitle();
	  		
	  		if(title == null) {
	  			_logger.error("Failed to extract title for " + base);
	  			return false;
	  		}
	  		
	  		String text = document.getText(true, false);

	  		webPage.setTitle(title);
	  		webPage.setText(text);
	  		webPage.setArticle(!isLowQuality);
	  		
	  		mediaItems.addAll(extractArticleImages(imgDoc, webPage, base, content));		
	  		webPage.setMedia(mediaItems.size());
	  		
	  		List<String> mediaIds = new ArrayList<String>();
	  		for(MediaItem mediaItem : mediaItems) {
	  			mediaIds.add(mediaItem.getId());
	  		}
	  		webPage.setMediaIds(mediaIds.toArray(new String[mediaIds.size()]));
	  		
	  		if(mediaItems.size() > 0) {
	  			MediaItem mediaItem = mediaItems.get(0);
	  			webPage.setMediaThumbnail(mediaItem.getUrl());
	  		}
	  		
			return true;
			
	  	} catch(Exception ex) {
	  		_logger.error(ex);
	  		return false;
	  	}
	}

	public Article getArticle(WebPage webPage, byte[] content) {  
		
		String base = webPage.getExpandedUrl();
		
	  	try { 
	  		InputSource articelIS1 = new InputSource(new ByteArrayInputStream(content));
	  		InputSource articelIS2 = new InputSource(new ByteArrayInputStream(content));
		  	TextDocument document = null, imgDoc = null;

	  		document = new BoilerpipeSAXInput(articelIS1).getTextDocument();
	  		imgDoc = new BoilerpipeSAXInput(articelIS2).getTextDocument();
	  		
	  		TextDocumentStatistics dsBefore = new TextDocumentStatistics(document, false);
	  		synchronized(_articleExtractor) {
	  			_articleExtractor.process(document);
	  		}
	  		synchronized(_extractor) {
	  			_extractor.process(imgDoc);
	  		}
	  		TextDocumentStatistics dsAfter = new TextDocumentStatistics(document, false);
	  		
	  		boolean isLowQuality = true;
	  		synchronized(_estimator) {
	  			isLowQuality = _estimator.isLowQuality(dsBefore, dsAfter);
	  		}
	  	
	  		String title = document.getTitle();
	  		String text = document.getText(true, false);
	  		
	  		Article article = new Article(title, text);
	  		article.setLowQuality(isLowQuality);
	  		
	  		List<MediaItem> mediaItems = extractArticleImages(imgDoc, webPage, base, content);		
	  		//List<MediaItem> mediaItems = extractAllImages(base, title, webPage, pageHash, content);
	  		
	  		for(MediaItem mItem : mediaItems) {
	  			article.addMediaItem(mItem);
	  		}
			return article;
			
	  	} catch(Exception ex) {
	  		_logger.error(ex);
	  		return null;
	  	}
	}
	
	public static List<MediaItem> extractArticleImages(TextDocument document, WebPage webPage, String base, byte[] content) 
			throws IOException, BoilerpipeProcessingException {
		
		List<MediaItem> mediaItems = new ArrayList<MediaItem>();
		
		InputSource imageslIS = new InputSource(new ByteArrayInputStream(content));
  		
  		List<Image> detectedImages;
  		synchronized(_imageExtractor) {
  			detectedImages = _imageExtractor.process(document, imageslIS);
  		}
  		
  		for(Image image  : detectedImages) {
  			Integer w = -1, h = -1;
  			try {
  				String width = image.getWidth().replaceAll("%", "");
  				String height = image.getHeight().replaceAll("%", "");
  	
  				w = Integer.parseInt(width);
  				h = Integer.parseInt(height);
  			}
  			catch(Exception e) {
  				// filter images without size
  				continue;
  			}
  			
  			// filter small images
  			if(image.getArea() < minArea || w < minDim  || h < minDim) 
				continue;

			String src = image.getSrc();
			URL url = null;
			try {
				url = new URL(new URL(base), src);
				
				if(url.toString().length() > maxUrlLength)
					continue;
				
				if(src.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
			} catch (Exception e) {
				_logger.error("Error for " + src + " in " + base);
				continue;
			}
			
			String alt = image.getAlt();
			if(alt == null) {
				alt = webPage.getTitle();
				if(alt == null)
					continue;
			}
			
			MediaItem mediaItem = new MediaItem(url);
			
			// Create image unique id. Is this a good practice? 
			int imageHash = (url.hashCode() & 0x7FFFFFFF);
			
			mediaItem.setId("Web#" + imageHash);
			mediaItem.setSource("Web");
			mediaItem.setType("image");
			mediaItem.setThumbnail(url.toString());
			
			mediaItem.setPageUrl(base.toString());
			mediaItem.setReference(webPage.getReference());
			
			mediaItem.setShares((long)webPage.getShares());
			
			mediaItem.setTitle(alt.trim());
			mediaItem.setDescription(webPage.getTitle());
			
			if(w != -1 && h != -1) 
				mediaItem.setSize(w, h);
			
			if(webPage.getDate() != null)
				mediaItem.setPublicationTime(webPage.getDate().getTime());
			
			mediaItems.add(mediaItem);
		}
  		return mediaItems;
	}
	
	
	public static List<MediaItem> extractAllImages(String baseUri, String title, WebPage webPage, byte[] content) throws IOException {
		List<MediaItem> images = new ArrayList<MediaItem>();
		
		String html = IOUtils.toString(new ByteArrayInputStream(content));
		Document doc = Jsoup.parse(html, baseUri);
		
		Elements elements = doc.getElementsByTag("img");
		for(Element img  : elements) {
  			
			String src = img.attr("src");
			String alt = img.attr("alt");
			String width = img.attr("width");
			String height = img.attr("height");
			
  			Integer w = -1, h = -1;
  			try {
  				if(width==null || height==null || width.equals("") || height.equals(""))
  					continue;
  				
  				w = Integer.parseInt(width);
  				h = Integer.parseInt(height);
  				
  				// filter small images
  	  			if( (w*h) < minArea || w < minDim  || h < minDim) 
  					continue;
  			}
  			catch(Exception e) {
  				_logger.error(e);
  			}

			URL url = null;
			try {
				url = new URL(src);
				
				if(url.toString().length() > maxUrlLength)
					continue;
				
				if(src.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
			} catch (Exception e) {
				_logger.error(e);
				continue;
			}
			
			if(alt == null) {
				alt = title;
			}
			
			MediaItem mediaItem = new MediaItem(url);
			
			// Create image unique id
			String urlStr = url.toString().trim();
			int imageHash = (urlStr.hashCode() & 0x7FFFFFFF);
			
			mediaItem.setId("Web#" + imageHash);
			mediaItem.setSource("Web");
			mediaItem.setType("image");
			mediaItem.setThumbnail(url.toString());
			
			mediaItem.setPageUrl(baseUri);
			
			mediaItem.setShares((long)webPage.getShares());
			mediaItem.setTitle(alt.trim());
			mediaItem.setDescription(webPage.getTitle());
			
			if(w != -1 && h != -1) 
				mediaItem.setSize(w, h);
			
			if(webPage.getDate() != null)
				mediaItem.setPublicationTime(webPage.getDate().getTime());
			
			images.add(mediaItem);
		}
		return images;
	}
	
	public static List<MediaItem> extractVideos(WebPage webPage, byte[] content) {

		String base = webPage.getExpandedUrl();
		
		List<MediaItem> videos = new ArrayList<MediaItem>(); 
		int pageHash = (base.hashCode() & 0x7FFFFFFF);
		try {
			Document doc = Jsoup.parse(new ByteArrayInputStream(content), "UTF-8", base);
			
			Elements objects = doc.getElementsByTag("object");
			
			System.out.println(objects.size()+" objects");
			
			for(Element object :objects) {
				System.out.println(object);
				String data = object.attr("data");
				if(data == null || data.equals("")) {
					System.out.println("data is null");
					continue;
				}
				try {
					URL url = new URL(data);
					MediaItem mediaItem = new MediaItem(url);
					
					int imageHash = (url.hashCode() & 0x7FFFFFFF);
					mediaItem.setId("Web#"+pageHash+"_"+imageHash);
					mediaItem.setSource("Web");
					mediaItem.setType("video");
					mediaItem.setThumbnail(url.toString());
					
					mediaItem.setPageUrl(base.toString());
					
					mediaItem.setShares((long) webPage.getShares());
				}
				catch(Exception e) {
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
			_logger.error(e);
		}
		
		return videos;
	}
}
