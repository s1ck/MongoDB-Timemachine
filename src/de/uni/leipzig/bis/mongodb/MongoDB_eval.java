package de.uni.leipzig.bis.mongodb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import de.uni.leipzig.bis.mongodb.MongoDB_Queries.DataType;

/**
 * This project is an evaluation of MongoDB for a usecase where time series data
 * (up to 30M measuring points) has to be stored and queried. Queries are simple
 * range queries (from - to timestamp) and simple filters, like
 * "all data from station x" or "all days where x was greater then y".
 * 
 * @author s1ck
 * 
 */
public class MongoDB_Eval {

	/**
	 * Database specific settings
	 */
	public static final String HOST = "localhost";
	public static final Integer PORT = 27017;
	public static final String DB = "BIS_mongo_eval";
	public static final String COLLECTION_MEASUREMENT = "measurings";
	public static final String COLLECTION_STATIONS = "stations";
	public static final Integer BUFFER_SIZE = 10000;

	/**
	 * Data paths
	 */
	public static final String PATH_380K = "data/380K.csv";
	public static final String PATH_6M = "data/6mio.csv";
	public static final String PATH_30M = "data/30nio.csv";

	public static void main(String[] args) throws UnknownHostException,
			MongoException {
		// open connection
		Mongo m = new Mongo(HOST, PORT);
		// chose database (will be created if not existing
		DB db = m.getDB(DB);

		// create collections (if not existing)
		if (db.getCollection(COLLECTION_MEASUREMENT) == null) {
			db.createCollection(COLLECTION_MEASUREMENT, null);
		}
		if (db.getCollection(COLLECTION_STATIONS) == null) {
			db.createCollection("stations", null);
		}

		// importMeasuringPoints(PATH_6M, db);
		testMeasurementData(db, false);

		// close connection
		m.close();
	}

	/**
	 * Performs a set of queries on the mongoDB instance.
	 * 
	 * @param mongoDB
	 *            the mongoDB instance to perform queries on
	 * @param useIndices
	 *            true if indices shall be used for the queries (takes time to
	 *            create / drop)
	 */
	private static void testMeasurementData(DB mongoDB, boolean useIndices) {
		// create indices
		if (useIndices) {
			MongoDB_Queries.createIndex(mongoDB, COLLECTION_MEASUREMENT,
					"timestamp");
			MongoDB_Queries.createIndex(mongoDB, COLLECTION_MEASUREMENT,
					"datatype");
			MongoDB_Queries.createIndex(mongoDB, COLLECTION_MEASUREMENT,
					"value");
		}

		// do some queries
		MongoDB_Queries.rangeQuery1(mongoDB, 1314277800000L, 1314282900000L);
		MongoDB_Queries.tresholdQuery1(mongoDB, DataType.GAIN, 20000);

		// drop indices
		if (useIndices) {
			MongoDB_Queries.dropIndex(mongoDB, COLLECTION_MEASUREMENT,
					"timestamp");
			MongoDB_Queries.dropIndex(mongoDB, COLLECTION_MEASUREMENT,
					"datatype");
			MongoDB_Queries.dropIndex(mongoDB, COLLECTION_MEASUREMENT, "value");
		}
	}

	/**
	 * Loads measuring data from a given CSV file into a mongoDB instance.
	 * 
	 * @param csvFile
	 *            Path (absolute or relative to the csv file)
	 * @param mongoDB
	 *            the mongoDB instance
	 */
	@SuppressWarnings("unused")
	private static void importMeasuringPoints(String csvFile, DB mongoDB) {
		List<BasicDBObject> dbObjectPuffer = new ArrayList<BasicDBObject>(
				BUFFER_SIZE);
		BufferedReader bf;
		try {
			bf = new BufferedReader(new FileReader(csvFile));

			String line;
			long n = 0L;

			System.out.println("importing data");
			long start = System.currentTimeMillis();

			while ((line = bf.readLine()) != null) {
				n++;
				dbObjectPuffer.add(createDBObjectFromData(line));
				if (dbObjectPuffer.size() == BUFFER_SIZE) {
					// store data and clear buffer
					writeDataToDB(dbObjectPuffer, mongoDB);
					dbObjectPuffer.clear();
					System.out.print(".");
					if (n / BUFFER_SIZE % 10 == 0)
						System.out.printf(" %d documents inserted\n", n);

				}
			}
			long diff = System.currentTimeMillis() - start;
			System.out.printf(
					"took %d seconds for %d documents (%d documents / s)",
					diff / 1000, n, n / (diff / 1000));
			bf.close();
		} catch (FileNotFoundException fEx) {
			System.err.println(fEx);
		} catch (IOException ioEx) {
			System.err.println(ioEx);
		}
	}

	/**
	 * Creates a Database Object from a given csv String
	 * 
	 * @param line
	 *            CSV data information about the database object
	 * @return the Database Object
	 */
	private static BasicDBObject createDBObjectFromData(String line) {
		String[] documentData = line.split(";");

		BasicDBObject dbObj = new BasicDBObject();
		// timestamp
		dbObj.put("timestamp", Long.parseLong(documentData[0]));
		// value
		dbObj.put("value", Integer.parseInt(documentData[1]));
		// identifier
		String identifier = documentData[2];
		if (identifier != null) {
			String[] identifierData = identifier.split("\\.");
			// station_ID (Anlagenname)
			dbObj.put("stationID", identifierData[0]);
			// partID (Bauteilart)
			dbObj.put("partID", identifierData[1]);
			// serial number (laufende Nummer)
			dbObj.put("serialNo", identifierData[2]);
			// datatype (Datenart)
			dbObj.put("datatype", identifierData[3]);
			// optional data
			if (identifierData[3].equals("pdc")
					|| identifierData[3].equals("udc")) {
				// "string"
				dbObj.put("opt1", identifierData[4]);
				// serial number 2
				dbObj.put("opt2", identifierData[5]);
			}
		}
		return dbObj;
	}

	/**
	 * writes a set of documents to the database
	 * 
	 * @param documents
	 *            a set of database objects (documents)
	 * @param mongoDB
	 *            mongoDB instance where the documents are stored
	 */
	private static void writeDataToDB(List<BasicDBObject> documents, DB mongoDB) {
		DBCollection measurementCollection = mongoDB
				.getCollectionFromString(COLLECTION_MEASUREMENT);

		for (DBObject dbObj : documents) {
			measurementCollection.save(dbObj);
		}
	}
}
