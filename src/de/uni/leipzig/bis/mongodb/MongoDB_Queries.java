package de.uni.leipzig.bis.mongodb;

import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import de.uni.leipzig.bis.mongodb.MongoDB_Config.DataType;

/**
 * A collection of MongoDB Queries on timeseries data.
 * 
 * @author s1ck
 * 
 */
public class MongoDB_Queries {
	/**
	 * Processes a range query between two given timestamps.
	 * 
	 * @param mongoDB
	 *            mongoDB instance
	 * @param fromTimestamp
	 *            the lower bound of the range
	 * @param toTimestamp
	 *            the upper bound of the range
	 */
	public static void rangeQuery(DB mongoDB, long fromTimestamp,
			long toTimestamp) {
		// get collection
		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		System.out.println("Query: Range Query 1");
		System.out.println("********************");
		System.out.printf("from:\t%s\n", new Date(fromTimestamp));
		System.out.printf("to:\t%s\n", new Date(toTimestamp));

		// range query
		BasicDBObject rangeQuery = new BasicDBObject();
		// greater than, lower than
		rangeQuery.put("timestamp", new BasicDBObject("$gt", fromTimestamp)
				.append("$lt", toTimestamp));

		long diff = processQuery(measCollection, rangeQuery);

		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************\n\n");
	}

	/**
	 * Returns all timestamp where a given value was above a given treshold
	 * 
	 * @param mongoDB
	 *            mongoDB instance
	 * @param datatype
	 *            see {@link DataType} for available DataTypes
	 * @param treshold
	 *            Treshold as integer value
	 */
	public static void tresholdQuery(DB mongoDB, DataType datatype,
			int treshold) {
		// get collection
		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		System.out.println("Query: Treshold Query 1");
		System.out.println("********************");
		System.out.printf("datatype:\t%s\n", datatype);
		System.out.printf("treshold:\t%d\n", treshold);

		BasicDBObject tresholdQuery = new BasicDBObject();
		tresholdQuery.put("datatype", datatype.toString());
		tresholdQuery.put("value", new BasicDBObject("$gt", treshold));

		long diff = processQuery(measCollection, tresholdQuery);

		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************\n\n");
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

		System.out.println("Query: Create Index");
		System.out.println("********************");
		System.out.printf("index name:\t%s\n", indexName);

		long start = System.currentTimeMillis();
		collection.ensureIndex(indexName);
		long diff = System.currentTimeMillis() - start;

		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************\n\n");
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

		System.out.println("Query: Drop Index");
		System.out.println("********************");
		System.out.printf("index name:\t%s\n", indexName);

		long start = System.currentTimeMillis();
		collection.dropIndex(indexName + "_1");
		long diff = System.currentTimeMillis() - start;

		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************\n\n");
	}

	/**
	 * Processes a given query including warmup and test runs.
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
	private static long processQuery(DBCollection coll, BasicDBObject query) {
		long[] times = new long[MongoDB_Config.RUNS];
		long start;
		long diff;

		// warmup
		for (int i = 0; i < MongoDB_Config.SKIPS; i++) {
			coll.find(query).count();
		}

		// testing
		for (int i = 0; i < MongoDB_Config.RUNS; i++) {
			start = System.currentTimeMillis();
			coll.find(query).count();
			diff = System.currentTimeMillis() - start;
			times[i] = diff;
		}

		long sum = 0L;
		for (int i = 0; i < times.length; i++) {
			sum += times[i];
		}

		return sum / times.length;
	}

}
