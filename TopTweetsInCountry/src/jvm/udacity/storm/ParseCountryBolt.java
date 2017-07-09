package udacity.storm;

import java.util.Map;

import org.jboss.netty.util.internal.SystemPropertyUtil;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import udacity.storm.tools.CountryLookup;

/**
 * @author Sridhar Somasani
 *
 * The Bolt parses latitude and longitude information to country code and emits tweet and country id
 * 
 * <p> If location data is not available in a tweet, the bolt emits a new stream <i>"no-geo-info-country-bolt"</i>
 *  for further processing</p>
 *
 */
public class ParseCountryBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2656625376654975533L;

	OutputCollector collector;
	
	CountryLookup clookup ;
	
	@Override
	public void prepare(
			Map map, 
			TopologyContext topologyContex, 
			OutputCollector outputCollector) {
		
		this.collector = outputCollector;
		clookup = new CountryLookup();
	}
	
	
	@Override
	public void execute(Tuple tuple) {
		String tweet = tuple.getStringByField("tweet");
		String lat = tuple.getStringByField("location").split(",")[0];
		String lon = tuple.getStringByField("location").split(",")[1];
		if(lat.equals("None") || lon.equals("None")){
			collector.emit("no-geo-info-country-bolt", tuple, new Values(tweet));
		}else{
		    double latitude = Double.parseDouble(lat);
		    double longitude = Double.parseDouble(lon);
		    String country_id = clookup.getCountryCodeByGeo(latitude, longitude);
		    collector.emit(new Values(tweet, country_id));
		}

	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet", "country-id"));
		declarer.declareStream("no-geo-info-country-bolt", new Fields("tweet"));
	}

}
