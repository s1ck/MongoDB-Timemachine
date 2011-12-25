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

	class StationInfo {
		private String name;

		private int wrCount;

		public String getName() {
			return name;
		}

		public int getWrCount() {
			return wrCount;
		}

		public StationInfo(String name, int wrCount) {
			this.name = name;
			this.wrCount = wrCount;
		}
	}

	/**
	 * Connection to a mongo or mongos instance
	 */
	private Mongo m;

	/**
	 * Reference to the database containing the collections
	 */
	private DB db;

	/**
	 * Path to the data to be imported
	 */
	private String dataPath;

	/**
	 * Information about the stations which can be used for random data
	 * retrieval. Stations depend on the dataset.
	 */
	private List<StationInfo> availableStations;

	private List<String> availableDataTypes;

	private Long lowerTimeBound;

	private Long upperTimeBound;

	/*
	 * Getter Setter
	 */

	public Mongo getM() {
		return m;
	}

	public DB getDb() {
		return db;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	/*
	 * Public methods
	 */

	public void initDatabase(String path) throws UnknownHostException,
			MongoException {
		// open connection
		m = new Mongo(MongoDB_Config.HOST, MongoDB_Config.PORT);
		// chose database (will be created if not existing
		db = m.getDB(MongoDB_Config.DB);
		// init the list
		availableStations = new ArrayList<StationInfo>();
		// init the available datatypes
		availableDataTypes = new ArrayList<String>();

		// create collections (if not existing)
		if (db.getCollection(MongoDB_Config.COLLECTION_MEASURINGS) == null) {
			db.createCollection(MongoDB_Config.COLLECTION_MEASURINGS, null);
		}
		if (db.getCollection(MongoDB_Config.COLLECTION_STATIONS) == null) {
			db.createCollection("stations", null);
		}

		dataPath = path;
	}

	@SuppressWarnings("unchecked")
	public void initStations() {
		DBCollection measurings = db
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);
		List<String> stationIDs = measurings
				.distinct(MongoDB_Config.STATION_ID);

		for (String station : stationIDs) {
			// get the distinct wr ID (serialNo) and count them
			int wrCount = measurings.distinct(MongoDB_Config.SERIAL_NO,
					new BasicDBObject(MongoDB_Config.STATION_ID, station))
					.size();

			// store the information
			availableStations.add(new StationInfo(station, wrCount));
		}
	}

	public void initTimeRange() {
		DBCollection measurings = db
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		lowerTimeBound = (Long) measurings.find()
				.sort(new BasicDBObject(MongoDB_Config.TIMESTAMP, 1)).limit(1)
				.next().get(MongoDB_Config.TIMESTAMP);

		upperTimeBound = (Long) measurings.find()
				.sort(new BasicDBObject(MongoDB_Config.TIMESTAMP, -1)).limit(1)
				.next().get(MongoDB_Config.TIMESTAMP);
	}

	@SuppressWarnings("unchecked")
	public void initDataTypes() {
		DBCollection measurings = db
				.getCollection(MongoDB_Config.COLLECTION_MEASURINGS);

		List<String> dataTypes = measurings.distinct(MongoDB_Config.DATATYPE);

		for (String dataType : dataTypes) {
			// store the information
			availableDataTypes.add(dataType);
		}
	}

	public void shutdown() {
		// close connection
		m.close();
	}

	public void createIndexes(DB mongoDB) {
		MongoDB_Queries.createIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, MongoDB_Config.TIMESTAMP);
		MongoDB_Queries.createIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, MongoDB_Config.DATATYPE);
		MongoDB_Queries.createIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, MongoDB_Config.VALUE);
	}

	public void dropIndexes(DB mongoDB) {
		MongoDB_Queries.dropIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, MongoDB_Config.TIMESTAMP);
		MongoDB_Queries.dropIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, MongoDB_Config.DATATYPE);
		MongoDB_Queries.dropIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, MongoDB_Config.VALUE);
	}

	/**
	 * Processes the queries
	 * 
	 * @param mongoDB
	 *            mongodb connection
	 */
	public void processQueries(DB mongoDB) {

		// query 1
		System.out.println("query 1");
		for (int currentRun = 0; currentRun < MongoDB_Config.RUNS; currentRun++) {
			MongoDB_Queries.query1(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 2
		System.out.println("query 2");
		for (int currentRun = 0; currentRun < MongoDB_Config.RUNS; currentRun++) {
			MongoDB_Queries.query2(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 3
		System.out.println("query 3");
		for (int currentRun = 0; currentRun < MongoDB_Config.RUNS; currentRun++) {
			MongoDB_Queries.query3(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 4
		System.out.println("query 4");
		for (int currentRun = 0; currentRun < MongoDB_Config.RUNS; currentRun++) {
			MongoDB_Queries.query4(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
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
	public void importData(String csvFile, DB mongoDB) {
		List<BasicDBObject> dbObjectPuffer = new ArrayList<BasicDBObject>(
				MongoDB_Config.BUFFER_SIZE);
		BufferedReader bf;
		try {
			bf = new BufferedReader(new FileReader(csvFile));

			String line;
			long n = 0L;

			long start = System.currentTimeMillis();
			long diff = 0L;
			while ((line = bf.readLine()) != null) {
				n++;
				dbObjectPuffer.add(createDBObjectFromData(line));
				if (dbObjectPuffer.size() == MongoDB_Config.BUFFER_SIZE) {
					// store data and clear buffer
					writeDataToDB(dbObjectPuffer, mongoDB);
					dbObjectPuffer.clear();
					// System.out.print(".");
					if (n / MongoDB_Config.BUFFER_SIZE % 10 == 0) {
						diff = System.currentTimeMillis() - start;
						// System.out.printf(" %d documents inserted in %d seconds\n",
						// n, diff);
						System.out.printf("%d;%d\n", n, diff);
					}
				}
			}
			diff = System.currentTimeMillis() - start;
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

	/*
	 * Private Helpers
	 */

	/**
	 * Creates a Database Object from a given csv String
	 * 
	 * @param line
	 *            CSV data information about the database object
	 * @return the Database Object
	 */
	private BasicDBObject createDBObjectFromData(String line) {
		String[] documentData = line.split(";");

		BasicDBObject dbObj = new BasicDBObject();
		// timestamp
		dbObj.put(MongoDB_Config.TIMESTAMP, Long.parseLong(documentData[0]));
		// value
		dbObj.put(MongoDB_Config.VALUE, Integer.parseInt(documentData[1]));
		// identifier
		String identifier = documentData[2];
		if (identifier != null) {
			String[] identifierData = identifier.split("\\.");
			// station_ID (Anlagenname)
			dbObj.put(MongoDB_Config.STATION_ID, identifierData[0]);
			// partID (Bauteilart)
			dbObj.put(MongoDB_Config.PART_ID, identifierData[1]);
			// serial number (laufende Nummer)
			dbObj.put(MongoDB_Config.SERIAL_NO,
					Integer.parseInt(identifierData[2]));
			// datatype (Datenart)
			dbObj.put(MongoDB_Config.DATATYPE, identifierData[3]);
			// optional data
			if (identifierData[3].equals(MongoDB_Config.PDC)
					|| identifierData[3].equals(MongoDB_Config.UDC)) {
				// "string"
				dbObj.put(MongoDB_Config.OPT_STRING, identifierData[4]);
				// serial number 2
				dbObj.put(MongoDB_Config.SERIAL_NO_2, identifierData[5]);
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
	private void writeDataToDB(List<BasicDBObject> documents, DB mongoDB) {
		DBCollection measurementCollection = mongoDB
				.getCollectionFromString(MongoDB_Config.COLLECTION_MEASURINGS);

		for (DBObject dbObj : documents) {
			measurementCollection.save(dbObj);
		}
	}

	/*
	 * Main
	 */

	/**
	 * Main program
	 * 
	 * @param args
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public static void main(String[] args) throws UnknownHostException,
			MongoException {
		MongoDB_Eval eval = new MongoDB_Eval();
		// initialize connection to mongodb and database
		System.out.println("initializing connection...");
		eval.initDatabase((args.length > 0) ? args[0]
				: MongoDB_Config.PATH_380K);

		// data import
		System.out.println("importing data...");
		eval.importData(eval.getDataPath(), eval.getDb());

		// create indexes (if not existing)
		System.out.println("creating indexes...");
		eval.createIndexes(eval.getDb());

		// check which stations are available in the dataset
		System.out.println("initializing available stations...");
		eval.initStations();

		// get the time range of the dataset
		System.out.println("initializing time range...");
		eval.initTimeRange();

		// get the available datatypes
		System.out.println("initializing available datatypes...");
		eval.initDataTypes();

		// process the benchmark
		System.out.println("processing the benchmark...");
		eval.processQueries(eval.getDb());

		// shutdown the connection
		eval.shutdown();
	}
}
