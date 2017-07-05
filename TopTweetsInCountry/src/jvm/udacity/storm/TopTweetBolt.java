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
 * <p> It maintains all the top tweets partitioned by country. It basically accomplishes three tasks:</p>
 * 
 * <p>1. Consolidate all ranks from previous steams and maintains one master map of country wise hashtag counts </p>
 * 
 * <p>2. Process all tweets without location information from {@link ParseCountryBolt} and stream <i>no-geo-info-country-bolt</i>. Since there is no location information
 *  it checks all the tags from all countries and emits country id of hashtag with maximum rank. If no hashtag of tweet matches with 
 *  that of ranked hashtags, the tweet will be discarded</p>
 *  
 *  <p>3. Process all tweets with country id from {@link ParseCountryBolt} and stream <i>default </i>. It checks if hashtag of the tweet matches
 *   with that of top N hashtags of the country. If there is a match, it emits country id of the tweet and count of the hashtag</p>
 *
 */
public class TopTweetBolt extends BaseRichBolt {

	
//	private static final Logger LOG = Logger.getLogger(TopTweetBolt.class);
	
	private static final long serialVersionUID = 8097792275246745762L;
	private OutputCollector collector;
	private Map<String, HashtagWrapper> countryHashCount;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		// save the collector to emit tuples
		collector = outputCollector;
		countryHashCount = new Hashtable<>();
	}

	@Override
	public void execute(Tuple tuple) {

		String componentId = tuple.getSourceComponent();
		String streamId = tuple.getSourceStreamId();
		
		// consolidate ranks from previous intermediate rankers
		if (componentId.equals("ranker")) {
			String country_id = tuple.getStringByField("country-id");
			HashtagWrapper tagMap = (HashtagWrapper) tuple.getValueByField("rankings");
			countryHashCount.put(country_id, tagMap);
			
			// process tweets with no location information
		}else if(streamId.equals("no-geo-info-country-bolt") && componentId.equals("country-bolt")){
			processNoGeoInfoTweets(tuple);
			
			// process tweets with location information
		}else if (componentId.equals("country-bolt") && streamId.equals("default")) {
			
			String country_id = tuple.getStringByField("country-id");
			String tweet = tuple.getStringByField("tweet").toLowerCase();
			String original_tweet = tweet;
			//tweet = tweet.replaceAll("[^a-zA-Z0-9# .,?!]+", "").toLowerCase();
			String delims = "[ .,?!]+";
			String[] tokens = tweet.split(delims);
			HashtagWrapper tagCount = countryHashCount.get(country_id);
			if(tagCount == null) return;
			for(String token : tokens){
				Long rank = tagCount.getTagRank(token);
				if(rank != -1L){
					collector.emit(new Values(country_id, original_tweet, rank.toString()));
					break;
				}
			}
		}

	}
	
	private void processNoGeoInfoTweets(Tuple tuple){
		String tweet = tuple.getStringByField("tweet");
		String delims = "[ .,?!]+";
		String [] tokens = tweet.split(delims);
		for(String token : tokens){
			Long maxRank = -1L;
			String maxCountryId = null;
			if(!token.startsWith("#")) continue;
			for(String countryId: countryHashCount.keySet()){
				HashtagWrapper tags = countryHashCount.get(countryId);
				long rank = tags.getTagRank(token.toLowerCase());
				if(!(rank == -1L) && maxRank < rank){
					maxRank = rank;
					maxCountryId = countryId;
				}
			}
			if(maxCountryId != null && maxRank > 0)
				collector.emit(new Values(maxCountryId, tweet, maxRank.toString()));
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("country-id", "tweet", "rank"));
	}
}
