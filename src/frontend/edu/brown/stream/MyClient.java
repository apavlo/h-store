package edu.brown.stream;

import java.io.File;

import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

import java.nio.charset.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;


public class MyClient {
	final int port;
	Catalog catalog;
	Client client;
	InputClientConnection icc;
	ServerSocket serverSocket;

	MyClient() {
		this.port = HStoreConstants.DEFAULT_PORT; //21212
		this.catalog = new Catalog();
		this.icc = this.getClientConnection("localhost");
		this.client = icc.client;
		try {
			this.serverSocket = new ServerSocket(6000);
		} catch (IOException e) {
			System.out.println("Error creating socket on port 6000");
			e.printStackTrace();
		}
	}

	//Take a json message from the socket and parse it for the procedure
	//name and arguments.  Make a call to the specified procedure and return
	//the array of VoltTables.
	public VoltTable [] callStoredProcedure(String s, MyClient myc) {
		JSONObject j;
		VoltTable [] results;
		System.out.println("Calling stored procedure");
		try {
			j = new JSONObject(s);
			String procedureName = j.getString("proc");
			JSONArray args = j.getJSONArray("args");
			if (args.length() == 0) {
				results = myc.client.callProcedure(procedureName).getResults();
				return results;
			}
			if (args.length() == 1) {
				results = myc.client.callProcedure(procedureName, args.get(0)).getResults();
				return results;
			}
			if (args.length() == 2) {
				results = myc.client.callProcedure(procedureName, args.get(0), args.get(1)).getResults();
				return results;
			}
			System.out.println(j.get("proc"));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<String> parseResults(VoltTable vt) {
		System.out.println("Parsing results from S-Store");
		JSONObject j = new JSONObject();
		ArrayList<String> s = new ArrayList<String>();
		final int colCount = vt.getColumnCount();
		vt.resetRowPosition();
		if (colCount == 1 && vt.getRowCount() == 1) {
			s.add(String.valueOf(vt.asScalarLong()));
			return s;
		}
		while (vt.advanceRow()) {
			for (int col = 0; col < colCount; col++) {
				switch(vt.getColumnType(col)) {
				case INTEGER: case BIGINT:
					try {
						j.put(vt.getColumnName(col), vt.getLong(col));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case STRING:
					try {
						j.put(vt.getColumnName(col), vt.getString(col));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case DECIMAL:
					try {
						j.put(vt.getColumnName(col), vt.getDecimalAsBigDecimal(col));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case FLOAT:
					try {
						j.put(vt.getColumnName(col), vt.getDouble(col));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				}
			}
			s.add(j.toString());
		}
		return s;
	}

	//Used to grab information about where S-Store is running and establish a
	//connection to the db
	private class InputClientConnection {
		final Client client;
		final String hostname;
		final int port;

		public InputClientConnection(Client client, String hostname, int port) {
			this.client = client;
			this.hostname = hostname;
			this.port = port;
		}
	} // CLASS

	private InputClientConnection getClientConnection(String host) {
		String hostname = null;
		int port = -1;

		// Fixed hostname
		if (host != null) {
			if (host.contains(":")) {
				String split[] = host.split("\\:", 2);
				hostname = split[0];
				port = Integer.valueOf(split[1]);
			} else {
				hostname = host;
				port = this.port;
			}
		}
		// Connect to random host and using a random port that it's listening on
		else if (this.catalog != null) {
			Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
			hostname = catalog_site.getHost().getIpaddr();
			port = catalog_site.getProc_port();
		}
		assert(hostname != null);
		assert(port > 0);

		System.out.println(String.format("Creating new client connection to %s:%d",
				hostname, port));
		Client client = ClientFactory.createClient(128, null, false, null);
		try {
			client.createConnection(null, hostname, port, "user", "password");
			System.out.println("BatchRunner: connection is ok ... ");
		} catch (Exception ex) {
			String msg = String.format("Failed to connect to HStoreSite at %s:%d", hostname, port);
			throw new RuntimeException(msg);
		}
		return new InputClientConnection(client, hostname, port);
	}

	public static void main(String [] args) {
		MyClient myc = new MyClient();
		JSONObject json = new JSONObject();
		try {
			json.put("type", "CONNECT");
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while (true) {
				String proc;
				String jsonMessage;
				ArrayList<String> rows = new ArrayList<String>();
				JSONObject j;
				JSONArray jsonArray = new JSONArray();
				Socket api = myc.serverSocket.accept();
				System.out.println("Connected to " + api.getInetAddress());
				System.out.println("Starting while loop");
				BufferedReader apiCall = new BufferedReader(new InputStreamReader(api.getInputStream()));
				System.out.println("Received input stream");
				proc = apiCall.readLine();
				VoltTable [] results = myc.callStoredProcedure(proc, myc);
				System.out.println(results[0].toString());
				for (VoltTable vt: results) {
					if (vt.getColumnCount() == 1 && vt.getRowCount() == 1) {
						j = new JSONObject();
						j.put("data", jsonArray);
						j.put("success", vt.asScalarLong());
						ArrayList<String>temp = new ArrayList<String>();
						temp.add(String.valueOf(vt.asScalarLong()));
						rows = temp;
					}
					else {
						rows = myc.parseResults(vt);
						int rownum = 1;
						for (String s: rows) {
							jsonArray.put(new JSONObject(s));
							System.out.println("Added a row to the json object");
							System.out.println(jsonArray.toString());
						}
						System.out.println(jsonArray.toString());
						j = new JSONObject();
						j.put("data", jsonArray);
						j.put("success", 1);
					}
					jsonMessage = j.toString();
					OutputStreamWriter out = new OutputStreamWriter(api.getOutputStream(), "UTF-8");
					out.write(jsonMessage, 0, jsonMessage.length());
					out.flush();
					System.out.println("Done sending rows");
					api.close();
				}
			}
		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.print("hello");
	}

}
