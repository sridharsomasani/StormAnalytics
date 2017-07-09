package udacity.storm;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.tuple.Tuple;

/**
 * @author Sridhar Somasani
 *
 * This is the implementation of {@link AbstractPartitionRankerBolt}.
 * 
 * <p> Implements {@link #updateRankingsWithTuple(Tuple)} and {@link #updateWith(String, HashtagWrapper)} </p>
 * 
 *  <p>{@link #updateRankingsWithTuple(Tuple)}: Implements how current partitions ranking should be updated
 *  whenever new stream of tuples are received </p>
 */
public class PartitionRankingsBolt extends AbstractPartitionRankerBolt{

	private static final long serialVersionUID = -1369800530256637509L;
	private static final Logger LOG = Logger.getLogger(PartitionRankingsBolt.class);
	
	public PartitionRankingsBolt() {
		super();
	}
	
	public PartitionRankingsBolt(int topN){
		super(topN);
	}
	
	public PartitionRankingsBolt(int topN, int emitFrequencyInSeconds){
		super(topN, emitFrequencyInSeconds);
	}



	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		String country_id= tuple.getStringByField("country-id");
		HashtagWrapper rankings = (HashtagWrapper) tuple.getValueByField("rankings");
		rankings = rankings.copy();
		rankings.setCount(count);
		updateWith(country_id,rankings);
	}
	
	void updateWith(String country_id, HashtagWrapper rankings){
		Map<String, HashtagWrapper> ranks = getRankings();
		if(ranks.get(country_id) == null){
			ranks.put(country_id, rankings);
		}else{
			HashtagWrapper currentRanks = getRankings().get(country_id);
			currentRanks.updateRankings(rankings);
		}
	}

	@Override
	Logger getLogger() {
		return LOG;
	}

}
