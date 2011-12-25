package de.uni.leipzig.bis.mongodb;

import java.util.List;
import java.util.Random;

import javax.xml.crypto.Data;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MapReduceCommand.OutputType;

import de.uni.leipzig.bis.mongodb.MongoDB_Config.DataType;
import de.uni.leipzig.bis.mongodb.MongoDB_Eval.StationInfo;

/**
 * A collection of MongoDB Queries on timeseries data.
 * 
 * @author s1ck
 * 
 */
public class MongoDB_Queries {

	private static Random r = new Random();

	/*
	 * Helper
	 */

	/**
	 * Returns the Unix Timestamp which is a specified number of days greater
	 * than the given one.
	 * 
	 * @param from
	 *            start unix timestamp
	 * @param days
	 *            number of days
	 * @return unix timestamp in n days
	 */
	private static Long getUpperBound(Long from, int days) {
		return from + (days * 24L * 60L * 60L * 1000L);
	}

	/**
	 * Returns a random Unix Timestamp in a given range.
	 * 
	 * @param from
	 *            lower bound
	 * @param to
	 *            upper bound
	 * @param excludeDays
	 *            if true, the defined range in config will be substracted from
	 *            the upperbound to achieve equal ranges
	 * @return timestamp in range
	 */
	private static Long getRandomInRange(Long from, Long to, boolean excludeDays) {
		if (excludeDays) {
			from -= (MongoDB_Config.DAYS * 24L * 60L * 60L * 1000L);
		}
		return from + nextLong(to - from);
	}

	/**
	 * Generates a random long value between 0 and the given upper bound. Taken
	 * from here:
	 * http://stackoverflow.com/questions/2546078/java-random-long-number
	 * -in-0-x-n-range
	 * 
	 * @param n
	 *            maximum excluded value
	 * @return
	 */
	private static long nextLong(long n) {
		long bits, val;
		do {
			bits = (r.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits - val + (n - 1) < 0L);
		return val;
	}

	/**
	 * 1 Wieviele Einträge hat Zeitreihe XY insgesamt/im Zeitintervall
	 * [von,bis]?
	 * 
	 * @param mongoDB
	 * @param availableStations
	 * @param availableDataTypes
	 * @param lowerTimeBound
	 * @param upperTimeBound
	 */
	public static void query1(DB mongoDB, List<StationInfo> availableStations,
			List<String> availableDataTypes, Long lowerTimeBound,
			Long upperTimeBound) {
		// get collection
		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		String dataType = availableDataTypes.get(r.nextInt(availableDataTypes
				.size()));
		StationInfo stationInfo = availableStations.get(r
				.nextInt(availableStations.size()));
		int serialNo = r.nextInt(stationInfo.getWrCount());
		Long lowerBound = getRandomInRange(lowerTimeBound, upperTimeBound, true);
		Long upperBound = getUpperBound(lowerBound, MongoDB_Config.DAYS);

		// range query
		BasicDBObject rangeQuery = new BasicDBObject();
		// identifier
		rangeQuery.put(MongoDB_Config.DATATYPE, dataType);
		rangeQuery.put(MongoDB_Config.STATION_ID, stationInfo.getName());
		rangeQuery.put(MongoDB_Config.SERIAL_NO, serialNo);
		// range
		rangeQuery.put(MongoDB_Config.TIMESTAMP, new BasicDBObject("$gt",
				lowerBound).append("$lt", upperBound));

		long diff = processQuery(measCollection, rangeQuery);

		System.out.println(String.format("%s;%d;%d;%d;%d",
				stationInfo.getName(), serialNo, lowerBound, upperBound, diff));
	}

	/**
	 * 2 Wie ist der Wert der Zeitreihe XY zum Zeitpunkt Z?
	 * 
	 * TODO: because of the random timestamps there are mostly no results found.
	 * Maybe I should chose a random timestamp based in the existing ones
	 * 
	 * @param mongoDB
	 * @param availableStations
	 * @param availableDataTypes
	 * @param lowerTimeBound
	 * @param upperTimeBound
	 */
	public static void query2(DB mongoDB, List<StationInfo> availableStations,
			List<String> availableDataTypes, Long lowerTimeBound,
			Long upperTimeBound) {
		// get collection
		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		String dataType = availableDataTypes.get(r.nextInt(availableDataTypes
				.size()));
		StationInfo stationInfo = availableStations.get(r
				.nextInt(availableStations.size()));
		int serialNo = r.nextInt(stationInfo.getWrCount());
		Long timestamp = getRandomInRange(lowerTimeBound, upperTimeBound, false);

		// exact match query
		BasicDBObject exactQuery = new BasicDBObject();
		// selection
		exactQuery.put(MongoDB_Config.DATATYPE, dataType.toString());
		exactQuery.put(MongoDB_Config.STATION_ID, stationInfo.getName());
		exactQuery.put(MongoDB_Config.SERIAL_NO, serialNo);
		exactQuery.put(MongoDB_Config.TIMESTAMP, timestamp);

		// projection
		BasicDBObject projection = new BasicDBObject();
		projection.put(MongoDB_Config.VALUE, 1);

		long diff = processQuery(measCollection, exactQuery, projection);
		System.out.println(String.format("%s;%d;%d;%d", stationInfo.getName(),
				serialNo, timestamp, diff));
	}

	/**
	 * 3 Wie sind die Werte der Zeitreihe XY im Zeitintervall [von,bis]?
	 * 
	 * @param mongoDB
	 * @param availableStations
	 * @param availableDataTypes
	 * @param lowerTimeBound
	 * @param upperTimeBound
	 */
	public static void query3(DB mongoDB, List<StationInfo> availableStations,
			List<String> availableDataTypes, Long lowerTimeBound,
			Long upperTimeBound) {
		// get random time series

		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		String dataType = availableDataTypes.get(r.nextInt(availableDataTypes
				.size()));
		StationInfo stationInfo = availableStations.get(r
				.nextInt(availableStations.size()));
		int serialNo = r.nextInt(stationInfo.getWrCount());
		Long lowerBound = getRandomInRange(lowerTimeBound, upperTimeBound, true);
		Long upperBound = getUpperBound(lowerBound, MongoDB_Config.DAYS);

		// build query

		BasicDBObject rangeQuery = new BasicDBObject();
		// identifier
		rangeQuery.put(MongoDB_Config.DATATYPE, dataType);
		rangeQuery.put(MongoDB_Config.STATION_ID, stationInfo.getName());
		rangeQuery.put(MongoDB_Config.SERIAL_NO, serialNo);
		// range
		rangeQuery.put(MongoDB_Config.TIMESTAMP, new BasicDBObject("$gt",
				lowerBound).append("$lt", upperBound));

		// projection
		BasicDBObject projection = new BasicDBObject();
		projection.put(MongoDB_Config.VALUE, 1);

		// run
		long diff = processQuery(measCollection, rangeQuery, projection);

		System.out.println(String.format("%s;%d;%d;%d;%d",
				stationInfo.getName(), serialNo, lowerBound, upperBound, diff));
	}

	/**
	 * 4 Wie ist der Zeitpunkt des ältesten/neuesten Eintrags in Zeitreihe XY?
	 * (neuester)
	 * 
	 * @param mongoDB
	 * @param availableStations
	 * @param availableDataTypes
	 * @param lowerTimeBound
	 * @param upperTimeBound
	 */
	public static void query4(DB mongoDB, List<StationInfo> availableStations,
			List<String> availableDataTypes, Long lowerTimeBound,
			Long upperTimeBound) {
		// get random time series

		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		String dataType = availableDataTypes.get(r.nextInt(availableDataTypes
				.size()));
		StationInfo stationInfo = availableStations.get(r
				.nextInt(availableStations.size()));
		int serialNo = r.nextInt(stationInfo.getWrCount());

		// build Query

		BasicDBObject query = new BasicDBObject();
		query.put(MongoDB_Config.DATATYPE, dataType);
		query.put(MongoDB_Config.STATION_ID, stationInfo.getName());
		query.put(MongoDB_Config.SERIAL_NO, serialNo);

		// projection
		BasicDBObject projection = new BasicDBObject();
		projection.put(MongoDB_Config.TIMESTAMP, 1);

		// run

		long start = System.currentTimeMillis();
		long result = (Long) measCollection.find(query, projection)
				.sort(new BasicDBObject(MongoDB_Config.TIMESTAMP, 1)).limit(1)
				.next().get(MongoDB_Config.TIMESTAMP);
		long diff = System.currentTimeMillis() - start;

		System.out.println(String.format("%s;%d;%d;%d", stationInfo.getName(),
				serialNo, result, diff));
	}

	/**
	 * 5 Wie ist der maximale/minimale/durchschnittliche Wert der Zeitreihe XY
	 * im Zeitintervall [von,bis]?
	 * 
	 * @param mongoDB
	 * @param availableStations
	 * @param availableDataTypes
	 * @param lowerTimeBound
	 * @param upperTimeBound
	 */
	public static void query5(DB mongoDB, List<StationInfo> availableStations,
			List<String> availableDataTypes, Long lowerTimeBound,
			Long upperTimeBound) {
		// get random time series

		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		String dataType = availableDataTypes.get(r.nextInt(availableDataTypes
				.size()));
		StationInfo stationInfo = availableStations.get(r
				.nextInt(availableStations.size()));
		int serialNo = r.nextInt(stationInfo.getWrCount());
		Long lowerBound = getRandomInRange(lowerTimeBound, upperTimeBound, true);
		Long upperBound = getUpperBound(lowerBound, MongoDB_Config.DAYS);

		// configure Map Reduce

		String map = "function() { emit(this.stationID, {total:this.value, count:1, avg:0, min:this.value, max:this.value});}";

		String reduce = "function(key, values) {"
				+ "var r = {total:0, count:0, avg:0, min:0, max:0};"
				+ " if(values.length > 0) {" + " r.min = values[0].min;"
				+ " r.max = values[0].max;" + " }"
				+ " values.forEach(function(v) {" + " r.total += v.total;"
				+ " r.count += v.count;" + " if(v.min < r.min) {"
				+ " r.min = v.min;" + " }" + " if(v.max > r.max) {"
				+ " r.max = v.max;" + " }" + "});" + " return r;" + " }";

		String finalize = "function(k, r) { if(r.count > 0) r.avg = r.total / r.count; return r; }";

		DBObject query = new BasicDBObject();
		query.put(MongoDB_Config.DATATYPE, dataType);
		query.put(MongoDB_Config.STATION_ID, stationInfo.getName());
		query.put(MongoDB_Config.SERIAL_NO, serialNo);
		query.put(MongoDB_Config.TIMESTAMP,
				new BasicDBObject("$gt", lowerBound).append("$lt", upperBound));

		MapReduceCommand mrCommand = new MapReduceCommand(measCollection, map,
				reduce, null, OutputType.INLINE, query);
		mrCommand.setFinalize(finalize);

		// run

		long start = System.currentTimeMillis();
		measCollection.mapReduce(mrCommand);
		long diff = System.currentTimeMillis() - start;

		System.out.println(String.format("%s;%d;%d;%d;%d",
				stationInfo.getName(), serialNo, lowerBound, upperBound, diff));
	}

	/**
	 * 6 Wie ist der Verlauf des Wirkungsgrades für den Wechselrichter XY im
	 * Zeitintervall [von,bis]? (Wirkungsgrad:=PAC/(Summe PDC aller Strings))
	 * 
	 * @param mongoDB
	 * @param availableStations
	 * @param availableDataTypes
	 * @param lowerTimeBound
	 * @param upperTimeBound
	 */
	public static void query6(DB mongoDB, List<StationInfo> availableStations,
			List<String> availableDataTypes, Long lowerTimeBound,
			Long upperTimeBound) {
		// get random time series

		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		StationInfo stationInfo = availableStations.get(r
				.nextInt(availableStations.size()));
		int serialNo = r.nextInt(stationInfo.getWrCount());
		Long lowerBound = getRandomInRange(lowerTimeBound, upperTimeBound, true);
		Long upperBound = getUpperBound(lowerBound, MongoDB_Config.DAYS);

		// configure Map Reduce

		String map = "function() { var r = {pac:0, total_pdc:0}; if(this.datatype == 'pac') { r.pac = this.value; } else { r.total_pdc = this.value;} emit(this.timestamp, r);}";

		String reduce = "function(key, values) { var r = {pac : 0, total_pdc:0}; values.forEach(function(v) { r.pac += v.pac; r.total_pdc += v.total_pdc;}); return r;}";

		String finalize = "function(k, r) { if(r.total_pdc > 0) { return {timestamp : k, wirkungsgrad : r.pac / r.total_pdc }; } else { return {timestamp : k, wirkungsgrad : 0 };}}";

		DBObject query = new BasicDBObject();
		// PDC or PAC
		BasicDBList list = new BasicDBList();
		list.add(DataType.PDC.toString());
		list.add(DataType.PAC.toString());
		
		query.put(MongoDB_Config.DATATYPE, new BasicDBObject("$in", list));
		query.put(MongoDB_Config.STATION_ID, stationInfo.getName());
		query.put(MongoDB_Config.SERIAL_NO, serialNo);
		query.put(MongoDB_Config.TIMESTAMP,
				new BasicDBObject("$gt", lowerBound).append("$lt", upperBound));

		MapReduceCommand mrCommand = new MapReduceCommand(measCollection, map,
				reduce, null, OutputType.INLINE, query);
		mrCommand.setFinalize(finalize);

		// run

		long start = System.currentTimeMillis();
		measCollection.mapReduce(mrCommand);		
		long diff = System.currentTimeMillis() - start;

		System.out.println(String.format("%s;%d;%d;%d;%d",
				stationInfo.getName(), serialNo, lowerBound, upperBound, diff));
	}

	/**
	 * Creates an index on the given attribute in the given collection.
	 * 
	 * @param mongoDB
	 * @param collectionName
	 * @param indexName
	 */
	public static void createIndex(DB mongoDB, String collectionName,
			String indexName) {
		// get collection
		DBCollection collection = mongoDB.getCollection(collectionName);

		long start = System.currentTimeMillis();
		collection.ensureIndex(indexName);
		long diff = System.currentTimeMillis() - start;

		System.out.println(String.format("%s;%d", indexName, diff));
	}

	/**
	 * Drops an index from an attribute at a given collection
	 * 
	 * @param mongoDB
	 * @param collectionName
	 * @param indexName
	 */
	public static void dropIndex(DB mongoDB, String collectionName,
			String indexName) {
		// get collection
		DBCollection collection = mongoDB.getCollection(collectionName);

		long start = System.currentTimeMillis();
		collection.dropIndex(indexName + "_1");
		long diff = System.currentTimeMillis() - start;

		System.out.println(String.format("%s;%d", indexName, diff));
	}

	private static long processQuery(DBCollection coll, BasicDBObject query) {
		return processQuery(coll, query, null);
	}

	/**
	 * Processes a given query and measures the duration.
	 * 
	 * @param coll
	 *            the collection to perform the query on
	 * @param query
	 *            the query
	 * @param skips
	 *            number of warmup runs
	 * @param runs
	 *            number of runs
	 * @return
	 */
	private static long processQuery(DBCollection coll, BasicDBObject query,
			BasicDBObject fields) {
		long start = System.currentTimeMillis();
		coll.find(query, fields).count();
		return System.currentTimeMillis() - start;
	}
}
