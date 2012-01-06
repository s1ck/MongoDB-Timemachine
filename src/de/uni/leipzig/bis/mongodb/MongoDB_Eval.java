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

import de.uni.leipzig.bis.mongodb.MongoDB_Config.DataType;

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
	/**
	 * In each run the original available stations will be added to the
	 * available stations with an attached run id. This is a workaround to avoid
	 * distinct queries before each run.
	 */
	private List<StationInfo> originalStations;

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
		originalStations = new ArrayList<StationInfo>();
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
			originalStations.add(new StationInfo(station, wrCount));
		}
	}

	/**
	 * Adds any original station (e.g. singwitz) plus the current run-id to the
	 * available stations, so these will be available in the next test run.
	 * 
	 * @param globalRun
	 */
	public void updateStations(int globalRun) {
		for (StationInfo station : originalStations) {
			availableStations.add(new StationInfo(station.getName().substring(
					0, station.getName().length() - 1)
					+ Integer.toString(globalRun), station.getWrCount()));
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

		// compound index on station, inverter, datatype and timestamp
		DBObject index = new BasicDBObject();
		index.put(MongoDB_Config.STATION_ID, 1);
		index.put(MongoDB_Config.SERIAL_NO, 1);
		index.put(MongoDB_Config.DATATYPE, 1);
		index.put(MongoDB_Config.TIMESTAMP, 1);

		MongoDB_Queries.createIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, index);
	}

	public void dropIndexes(DB mongoDB) {

		DBObject index = new BasicDBObject();
		index.put(MongoDB_Config.STATION_ID, 1);
		index.put(MongoDB_Config.SERIAL_NO, 1);
		index.put(MongoDB_Config.DATATYPE, 1);
		index.put(MongoDB_Config.TIMESTAMP, 1);

		MongoDB_Queries.dropIndex(mongoDB,
				MongoDB_Config.COLLECTION_MEASURINGS, index);
	}

	/**
	 * Processes the queries
	 * 
	 * @param mongoDB
	 *            mongodb connection
	 */
	public void processQueries(DB mongoDB) {

		MongoDB_Queries.initRandom();

		// query 1
		System.out.println("query 1");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query1(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 2
		System.out.println("query 2");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query2(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 3
		System.out.println("query 3");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query3(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 4
		System.out.println("query 4");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query4(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 5
		System.out.println("query 5");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query5(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 6
		System.out.println("query 6");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query6(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 7
		System.out.println("query 7");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query7(mongoDB, availableStations,
					availableDataTypes, lowerTimeBound, upperTimeBound);
		}

		// query 8
		System.out.println("query 8");
		for (int currentRun = 0; currentRun < MongoDB_Config.LOCAL_RUNS; currentRun++) {
			MongoDB_Queries.query8(mongoDB, availableStations,
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
	public void importData(String csvFile, DB mongoDB, int globalRun) {
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
				dbObjectPuffer.add(createDBObjectFromData(line, globalRun));
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
					"took %d seconds for %d documents (%d documents / s)\n",
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
	private BasicDBObject createDBObjectFromData(String line, int globalRun) {
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
			dbObj.put(MongoDB_Config.STATION_ID,
					String.format("%s_%d", identifierData[0], globalRun));
			// partID (Bauteilart)
			dbObj.put(MongoDB_Config.PART_ID, identifierData[1]);
			// serial number (laufende Nummer)
			dbObj.put(MongoDB_Config.SERIAL_NO,
					Integer.parseInt(identifierData[2]));
			// datatype (Datenart)
			dbObj.put(MongoDB_Config.DATATYPE, identifierData[3]);
			// optional data
			if (identifierData[3].equals(DataType.PDC.toString())
					|| identifierData[3].equals(DataType.UDC.toString())) {
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

		// init the configuration for the benchmark
		MongoDB_Config.initConfig((args.length > 0) ? args[0]
				: "config/mongodb.properties");

		// initialize connection to mongodb and database
		System.out.println("initializing connection...");
		eval.initDatabase(MongoDB_Config.PATH);

		// create indexes (if not existing)
		System.out.println("creating indexes...");
		eval.createIndexes(eval.getDb());

		for (int globalRun = 1; globalRun <= MongoDB_Config.GLOBAL_RUNS; globalRun++) {

			System.out.println(String.format(
					"Starting global run %d using dataset %s", globalRun,
					MongoDB_Config.PATH));

			if (MongoDB_Config.IMPORT) {
				// data import
				System.out.println("importing data...");
				eval.importData(eval.getDataPath(), eval.getDb(), globalRun);
			}

			if (globalRun == 1) { // only necessary after the first import

				// check which stations are available in the dataset
				System.out.println("initializing available stations...");
				eval.initStations();

				// get the time range of the dataset
				System.out.println("initializing time range...");
				eval.initTimeRange();

				// get the available datatypes
				System.out.println("initializing available datatypes...");
				eval.initDataTypes();
			} else {
				System.out.println("updating available stations...");
				eval.updateStations(globalRun);
			}

			// process the benchmark
			System.out.println("processing the benchmark...");
			eval.processQueries(eval.getDb());
		}

		// shutdown the connection
		eval.shutdown();
	}
}
