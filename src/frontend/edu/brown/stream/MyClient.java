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
	Socket api; //Connection to the Rest API
	//BufferedReader apiCall; //Reads messages from the Rest API
	InputStreamReader apiCall;
	
	MyClient() {
		this.port = HStoreConstants.DEFAULT_PORT; //21212
		this.catalog = new Catalog();
		this.reconnect();
		try {
			this.serverSocket = new ServerSocket(6000);
		} catch (IOException e) {
			System.out.println("Error creating socket on port 6000");
			e.printStackTrace();
		}
	}
	
	public void reconnect() {
		boolean connected = false;
		while (!connected) {
			try {
				this.icc = this.getClientConnection("localhost");
				connected = true;
			}
			catch (RuntimeException e) {
				//e.printStackTrace();
				//System.out.println(e.getMessage());
				connected = false;
			}
		}
		this.client = this.icc.client;
	}

	//Take a json message from the socket and parse it for the procedure
	//name and arguments.  Make a call to the specified procedure and return
	//the array of VoltTables.
	public VoltTable [] callStoredProcedure(String s) throws JSONException {
		JSONObject j;
		VoltTable [] results;
		System.out.println("Calling stored procedure");
		try {
			j = new JSONObject(s);
			String procedureName = j.getString("proc");
			JSONArray args = j.getJSONArray("args");
			ArrayList<Object> conversionList = new ArrayList<Object>();
			for (int i = 0; i < args.length(); i++) {
				conversionList.add(args.get(i));
			}
			Object [] argumentList = conversionList.toArray();
			results = client.callProcedure(procedureName, argumentList).getResults();
			return results;
		} catch (JSONException e) {
			if (s == null)
				System.out.println("Received null string from client");
			else {
				System.out.println("JSON object missing expected fields");
			}
			e.printStackTrace();
		} catch (NoConnectionsException e) {
			System.out.println("Connection to S-Store was lost");
			e.printStackTrace();
			this.reconnect();
			return this.callStoredProcedure(s);
		} catch (IOException e) {
			if (e.getMessage() != null)
				System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (ProcCallException e) {
			System.out.println(e.getMessage());
			JSONObject error = new JSONObject();
			error.put("data", "");
			error.put("error", e.getMessage().split("\n")[0]);
			error.put("success", 0);
			sendJSON(error);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			JSONObject error = new JSONObject();
			error.put("data", "");
			error.put("error", e.getMessage());
			error.put("success", 0);
			sendJSON(error);
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
		/*if (colCount == 1 && vt.getRowCount() == 1) {
			s.add(String.valueOf(vt.asScalarLong()));
			return s;
		}*/
		while (vt.advanceRow()) {
			for (int col = 0; col < colCount; col++) {
				switch(vt.getColumnType(col)) {
				case INTEGER: case BIGINT:
					try {
						j.put(vt.getColumnName(col), vt.getLong(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				case STRING:
					try {
						j.put(vt.getColumnName(col), vt.getString(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				case DECIMAL:
					try {
						j.put(vt.getColumnName(col), vt.getDecimalAsBigDecimal(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				case FLOAT:
					try {
						j.put(vt.getColumnName(col), vt.getDouble(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				}
			}
			s.add(j.toString());
		}
		return s;
	}
	
	public String readString() {
		String procedureName;
		System.out.println("Reading in a line from " + api.toString());
		try {
			while ((procedureName = findNewLine()) == null) {
				System.out.println("Received a null string");
				api.close();
				api = serverSocket.accept(); //Client likely disconnected
				System.out.println("Connected to " + api.getInetAddress());
				//apiCall = new BufferedReader(new InputStreamReader(api.getInputStream()));
				apiCall = new InputStreamReader(api.getInputStream(), "UTF-8");
			}
			return procedureName;
		} catch (IOException e) {
			System.out.println("Unable to read from the Rest client");
			e.printStackTrace();
		}
		return null;
	}
	
	public String findNewLine() {
		String procedureName = "";
		int c;
		System.out.println(apiCall.getEncoding());
		boolean found = false;
		try {
			while (!found) {
				c = apiCall.read();
				if ((char) c == '\n') {
					System.out.println(procedureName);
					return procedureName += (char) c;
				}
				if (c == -1)
					return null;
				procedureName += (char) c;
				System.out.println(procedureName);
			}
			return procedureName;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public void sendJSON(JSONObject j) {
		String jsonMessage = (j.toString() + "\n");
		System.out.println(jsonMessage);
		try {
			OutputStreamWriter out = new OutputStreamWriter(api.getOutputStream(), "UTF-8");
			out.write(jsonMessage, 0, jsonMessage.length());
			out.flush();
		} catch (IOException e) {
			System.out.println("Unable to write to the Rest client");
			e.printStackTrace();
		}
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

		//System.out.println(String.format("Creating new client connection to %s:%d",
				//hostname, port));
		Client client = ClientFactory.createClient(128, null, false, null);
		try {
			client.createConnection(null, hostname, port, "user", "password");
			System.out.println("BatchRunner: connection is ok ... ");
		} catch (Exception ex) {
			String msg = String.format("Failed to connect to HStoreSite at %s:%d", hostname, port);
			try {
				client.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			throw new RuntimeException(msg);
		}
		return new InputClientConnection(client, hostname, port);
	}
	
	public static void main(String [] args) {
		String proc;
		JSONObject j;
		VoltTable [] results;
		MyClient myc = new MyClient();
		try {
			//myc.client.callProcedure("SignUp", 1001);
			myc.api = myc.serverSocket.accept();
			System.out.println("Connected to " + myc.api.getInetAddress());
			//myc.apiCall = new BufferedReader(new InputStreamReader(myc.api.getInputStream()));
			myc.apiCall = new InputStreamReader(myc.api.getInputStream(), "UTF-8");
			while (true) {
				ArrayList<String> rows = new ArrayList<String>();
				JSONArray jsonArray = new JSONArray();
				proc = myc.readString();
				System.out.println("Received input stream");
				while ((results = myc.callStoredProcedure(proc)) == null) {
					proc = myc.readString();
				}
				j = new JSONObject();
				for (VoltTable vt: results) {
					if (vt.hasColumn("")) {
						long error = vt.asScalarLong();
						j.put("data", jsonArray);
						if (error < 0) {//Currently a false positive
							j.put("error", "DB error");
							j.put("success", 0);
						}
						else
							j.put("error", "");
							j.put("success", 1);
						rows.add(String.valueOf(vt.asScalarLong()));
					}
					else {
						for (String s: myc.parseResults(vt)) {
							jsonArray.put(new JSONObject(s));
						}
						j.put("data", jsonArray);
						j.put("error", "");
						j.put("success", 1);
					}
					System.out.println("Sending json to " + myc.api.toString());
					System.out.println("Called procedure " + proc);
					myc.sendJSON(j);
					System.out.println("Done sending rows");
				}
			}
		} catch (NoConnectionsException e) {
			System.out.println("Failure to create S-Store client connection");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unable to connect to Rest client");
			e.printStackTrace();
		} catch (JSONException e) {
			// This exception should never get thrown.
			// put() throws this in the event of a null string or an incorrect type being passed
			// as an argument.  The arguments are hard coded, so something catastrophic would have
			// to occur.
			e.printStackTrace();
		}/* catch (ProcCallException e) {
			System.out.println("Defined userid is already taken");
			e.printStackTrace();
		}*/
	}
}
