package de.uni.leipzig.bis.mongodb;

import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoDB_Queries {

	public static void rangeQuery1(DB mongoDB, long fromTimestamp,
			long toTimestamp) {
		// get collection
		DBCollection measCollection = mongoDB
				.getCollection(MongoDB_eval.COLLECTION_MEASUREMENT);
						
		System.out.println("Query: Range Query 1");
		System.out.println("********************");
		System.out.printf("from:\t%s\n", new Date(fromTimestamp));
		System.out.printf("to:\t%s\n", new Date(toTimestamp));				 		
		
		// range query
		BasicDBObject rangeQuery = new BasicDBObject();		
		// greater than, lower than
		rangeQuery.put("timestamp", new BasicDBObject("$gt", fromTimestamp).append("$lt", toTimestamp));

		long start = System.currentTimeMillis();
		measCollection.find(rangeQuery).count();
		long diff = System.currentTimeMillis() - start;

		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************");				
	}
	
	public static void createIndex(DB mongoDB, String collectionName, String indexName) {
		// get collection
		DBCollection collection = mongoDB
				.getCollection(collectionName);
		
		System.out.println("Query: Create Index");
		System.out.println("********************");
		
		long start = System.currentTimeMillis();
		collection.ensureIndex(indexName);
		long diff = System.currentTimeMillis() - start;
		
		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************");		
	}
	
	public static void dropIndex(DB mongoDB, String collectionName, String indexName) {
		// get collection
		DBCollection collection = mongoDB
				.getCollection(collectionName);
		
		System.out.println("Query: Drop Index");
		System.out.println("********************");
		
		long start = System.currentTimeMillis();
		collection.dropIndex(indexName + "_1");
		long diff = System.currentTimeMillis() - start;
		
		System.out.printf("took [ms]:\t%d\n", diff);
		System.out.println("********************");	
	}
	
	

}
