package udacity.storm;

import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Sridhar Somasani
 * 
 * Counts tags partitioned by each country
 *
 */
public class CountryHashtagCounter extends BaseRichBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1369800530256637349L;

	private static final Logger LOG = Logger.getLogger(CountryHashtagCounter.class);
	
	// To output tuples from this bolt to the next stage bolts, if any
	private OutputCollector collector;

	// Map to store the count of the words
	private Map<String, Hashtable<String, Long>> countMap;
	
	public void prepare(
	    Map                     map,
	    TopologyContext         topologyContext,
	    OutputCollector         outputCollector)
	{
	
	  // save the collector for emitting tuples
	  collector = outputCollector;
	
	  // create and initialize the map
	  countMap = new Hashtable<>();
	}
	
	@Override
	public void execute(Tuple tuple) {
	    String hashtag = tuple.getStringByField("hashtag");
	    String country_id = tuple.getStringByField("country-id");
	    Hashtable<String, Long> hashtagMap = null;
	    
	    // check for countryID and update tag count in the specific country
	    if(countMap.get(country_id) == null){
			hashtagMap = new Hashtable<>();
			hashtagMap.put(hashtag, 1L);
			countMap.put(country_id, hashtagMap);
	    }else{
	    	hashtagMap = countMap.get(country_id);
	    	if(hashtagMap.get(hashtag) == null){
	    		hashtagMap.put(hashtag, 1L);
	    	}else{
	    		long count = hashtagMap.get(hashtag) + 1L;
	    		hashtagMap.put(hashtag, count);
	    	}
	    	countMap.put(country_id, hashtagMap);
	    }
	    collector.emit(new Values(country_id, new HashtagWrapper(country_id, countMap.get(country_id))));
		
	}
	
	public Logger getLogger(){
		return LOG;
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
	{
		outputFieldsDeclarer.declare(new Fields("country-id", "rankings"));
	}

}
