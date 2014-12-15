package gr.iti.mklab.focused.crawler.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;

public class GeoProcessor extends Processor {
	
	private Map<String, String> timezoneToCountry = new HashMap<String, String>();
	
	public GeoProcessor(Configuration configuration) {
		super(configuration);
		
		if(timezones.length == countries.length) {
			for(int i=0; i<timezones.length; i++) {
				timezoneToCountry.put(timezones[i], countries[i]);
			}
		}
	}

	@Override
	public void process(Item item) {
		try {
			Location location = item.getLocation();
			if(location != null) {
				return;
			}
			
			Location loc = null;
			StreamUser streamUser = item.getStreamUser();
			if(streamUser != null) {
				
				String timezone = streamUser.getTimezone();
					
				if(timezone == null)
					return;
					
				String country = timezoneToCountry.get(timezone);
				loc = new Location(country);
				loc.setCountryName(country);
			}
			
			if(loc == null) {
				return;
			}
			
			item.setLocation(loc);
						
			List<MediaItem> mItems = item.getMediaItems();
			if(mItems != null) {
				for(MediaItem mItem : mItems) {
					Location miLoc = mItem.getLocation();
					if(miLoc == null) {
						mItem.setLocation(loc);
					}
				}
			}
		}
		catch(Exception e) {
			Logger.getLogger(GeoProcessor.class).error(e);
		}
	}

	
	private static String[] timezones = {
		"Abu Dhabi","Adelaide","Alaska","Almaty","Amsterdam","Arizona","Astana","Athens","Atlantic Time (Canada)",
		"Auckland","Azores","Baghdad","Baku","Bangkok","Beijing","Belgrade","Berlin","Bern","Bogota","Brasilia",
		"Bratislava","Brisbane","Brussels","Bucharest","Budapest","Buenos Aires","Cairo","Canberra","Cape Verde Is.",
		"Caracas","Casablanca","Central America","Central Time (US & Canada)","Chennai","Chihuahua","Chongqing",
		"Copenhagen","Darwin","Dhaka","Dublin","Eastern Time (US & Canada)","Edinburgh","Ekaterinburg","Fiji",
		"Georgetown","Greenland","Guadalajara","Guam","Hanoi","Harare","Hawaii","Helsinki","Hobart",
		"Hong Kong","Indiana (East)","International Date Line West","Irkutsk","Islamabad","Istanbul",
		"Jakarta","Jerusalem","Kabul","Kamchatka","Karachi","Kathmandu","Kolkata","Krasnoyarsk","Kuala Lumpur",
		"Kuwait","Kyiv","La Paz","Lima","Lisbon","Ljubljana","London","Madrid","Magadan","Marshall Is.",
		"Mazatlan","Melbourne","Mexico City","Mid-Atlantic","Midway Island","Minsk","Monrovia",
		"Monterrey","Moscow","Mountain Time (US & Canada)","Mumbai","Muscat","Nairobi","New Caledonia","New Delhi",
		"Newfoundland","Novosibirsk","Nuku'alofa","Osaka","Pacific Time (US & Canada)","Paris,Perth","Port Moresby",
		"Prague","Pretoria","Quito","Rangoon","Riga","Riyadh","Rome","Samoa","Santiago","Sapporo","Sarajevo",
		"Saskatchewan","Seoul","Singapore","Skopje","Sofia","Solomon Is.","Sri Jayawardenepura","St. Petersburg",
		"Stockholm","Sydney","Taipei","Tallinn","Tashkent","Tbilisi","Tehran","Tijuana","Tokyo","Ulaan Bataar","Urumqi",
		"Vienna","Vilnius","Vladivostok","Volgograd","Warsaw","Wellington","West Central Africa","Yakutsk",
		"Yerevan","Zagreb"};   

	private static String[] countries = {
		"United Arab Emirates","Australia","United States","Kazakhstan","Netherlands","United States","Kazakhstan","Greece","Canada",
		"New Zealand","Portugal","Iraq","Azerbaijan","Thailand","China","Serbia","Germany","Switzerland","Colombia","Brazil",
		"Slovakia","Australia","Belgium","Romania","Hungary","Argetina","Egypt","Australia","Cape Verde",
		"Venezuela","United Kingdom","United States","United States","India","Mexico","China",
		"Denmark","Australia","Bangladesh","Ireland","United States","United Kingdom","Russia","Fiji",
		"United States","Greenland","Mexico","United States","Vietnam","Zimbabwe","United States","Finland","Australia",
		"China","United States","United States","Russia","Pakistan","Turkey",
		"Indonesia","Israel","Afghanistan","Russia","Pakistan","Nepal","United States","Russia","Malaysia",
		"Kuwait","Russia","Volivia","Peru","Portugal","Slovenia","United Kingdom","Spain","Russia","United States",
		"Mexico","Australia","Mexico","United States","United States","Belarus","Liberia",
		"Mexico","Russia","United States","India","Oman","Kenya","France","India",
		"Canada","Russia","United States","Japan","United States","France","Papua New Guinea",
		"Czech","South Africa","Ecuador","Burma","Latvia","Saudi Arabia","Italy","New Zealand","Chile","Japan","Bosnia and Herzegovina",
		"Canada"," South Korea","Malaysia","Macedonia","Bulgary","United States","Sri Lanka","Russia",
		"Sweden ","Australia","Taiwan","Estonia","Uzbekistan","Georgia","Iran","Mexico","Japan","Mongolia","China",
		"Austria","Lithuania","Russia","Russia","Poland","New Zealand","United States","Russia",
		"Armenia","Croatia"}; 
}
