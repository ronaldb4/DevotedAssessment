package com.ronaldbuchanan.assessment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Stack;

import org.javatuples.Triplet;

public class ToyInMemDB {

	public static void main(String[] args) {
		try {
			boolean debug = args.length>0 && "-debug".equals(args[0].toLowerCase());
			ToyInMemDB db = new ToyInMemDB();
			
			System.out.println("Starting ... ");
			try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));){
				while(true) {
					String sysin = br.readLine();
					if (sysin==null || sysin.trim().length()==0)
						continue;
					
					String[] input = sysin.split(" ");
					try {
						switch (input[0].toUpperCase()) {
						case "END": System.out.println("session complete, terminating ..."); return;
						case "SET": 		db.set(input);		break;
						case "GET": 		db.get(input);		break;
						case "DELETE":		db.delete(input);	break;
						case "COUNT":		db.count(input);	break;
						case "BEGIN":		db.begin(input); 	break;
						case "ROLLBACK":	db.rollback(input);break;
						case "COMMIT":		db.commit(input);	break;
						case "DUMP":		if (debug) { db.dump(); break; }
						default:
							System.out.println("unrecognized function: " + input[0]);
							continue;
						}
					} catch (BadInput e) {
						System.err.print(e.getMessage());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** database - a simple key-value map where key=name, value=value */
	private final HashMap<String,String> database = new HashMap<>();
	
	/** index - maps from values to the names containing them where key=value and value=HashSet of names */
	private final HashMap<String,Integer> valueCount = new HashMap<>();
	
	/** transactionLog - maintains the outstanding transactions */
	private final HashMap<Integer,Stack<Triplet<String,String,String>>> transactionLog = new HashMap<>();
	private Integer currentTxId = 0;
	
	public ToyInMemDB() {
	}
	
	/**
	 * set the value for a name
	 * @param input
	 * @throws BadInput
	 */
	public void set(String[] input) throws BadInput {
		if (input.length!=3)
			throw new BadInput("improper command: SET accepts 2 parameters: [name] and [value]");

		String name = input[1];
		String newValue = input[2];
		String oldValue = database.getOrDefault(name, null);
		
		if (newValue.equals(oldValue))
			return; //no change, it's a push
		
		//log to the transaction log
		if (currentTxId>0)
			transactionLog.get(currentTxId).push(Triplet.with(name, oldValue, newValue));
		
		//update the database
		database.put(name, newValue);
		
		//update the index
		if (oldValue!=null)
			valueCount.put(oldValue, valueCount.getOrDefault(oldValue, 0)-1);
		valueCount.put(newValue, 1+valueCount.getOrDefault(newValue, 0));
	}
	
	/**
	 * delete a name
	 * @param input
	 * @throws BadInput
	 */
	public void delete(String[] input) throws BadInput {
		if (input.length!=2)
			throw new BadInput("improper command: DELETE accepts 1 parameter: [name]");

		String name = input[1];
		if (database.containsKey(name)) {
			String oldValue = database.get(name);
			String newValue = null;
			
			//log to the transaction log
			if (currentTxId>0)
				transactionLog.get(currentTxId).push(Triplet.with(name, oldValue, newValue));
			
			//update the index
			valueCount.put(oldValue, valueCount.get(oldValue)-1);
			
			//update the database
			database.remove(name);
		}
	}
	
	/**
	 * get the value for a name
	 * @param input
	 * @throws BadInput
	 */
	public void get(String[] input) throws BadInput {
		if (input.length!=2)
			throw new BadInput("improper command: GET accepts 1 parameter: [name]");
			
		System.out.println(database.getOrDefault(input[1],"NULL"));
	}
	
	/**
	 * count the occurrences of a value
	 * @param input
	 * @throws BadInput
	 */
	public void count(String[] input) throws BadInput {
		if (input.length!=2)
			throw new BadInput("improper command: COUNT accepts 1 parameter: [value]");

		System.out.println(valueCount.getOrDefault(input[1], 0));
	}
	
	/**
	 * start a new transaction
	 * @param input
	 * @throws BadInput
	 */
	public void begin(String[] input) throws BadInput {
		if (input.length!=1)
			throw new BadInput("improper command: BEGIN does not accept any parameters");
		
		//insert new stack for transaction log and increment the current transaction id 
		transactionLog.put(++currentTxId, new Stack<>());
	}
	
	/**
	 * rollback the current transaction
	 * @param input
	 * @throws BadInput
	 */
	public void rollback(String[] input) throws BadInput {
		if (input.length!=1)
			throw new BadInput("improper command: ROLLBACK does not accept any parameters");
		
		if (currentTxId==0) {
			System.out.println("TRANSACTION NOT FOUND");
			return;
		}

		Stack<Triplet<String,String,String>> tx = transactionLog.get(currentTxId);
		while (!tx.empty()) { // definitely O(n)
			Triplet<String,String,String> entry = tx.pop();
			String name = entry.getValue0();
			String oldValue = entry.getValue1(); 
			String newValue = entry.getValue2();
			
			//update the database
			if (oldValue == null)
				database.remove(name);
			else
				database.put(name, oldValue);
			
			//update the count
			if (newValue!=null)
				valueCount.put(newValue, valueCount.get(newValue)-1);
			valueCount.put(oldValue, 1 + valueCount.getOrDefault(oldValue, 0));
		}
		
		//remove the entries from the current transaction and decrement the current transaction id
		transactionLog.remove(currentTxId--);
	}
	
	/**
	 * commit all outstanding transactions
	 * @param input
	 * @throws BadInput
	 */
	public void commit(String[] input) throws BadInput {
		if (input.length!=1)
			throw new BadInput("improper command: COMMIT does not accept any parameters");
		
		//commits ALL outstanding transactions - these are already in the database, so just clear out the log 
		transactionLog.clear();
		currentTxId = 0;
	}
	
	/**
	 * print out the contents of the data structures
	 */
	private void dump() {
		System.out.println("database entries");
		for (String name : database.keySet())
			System.out.println("\t"+name+"="+database.get(name));
		
		System.out.println("index entries");
		for (String value : valueCount.keySet())
			System.out.println("\t"+value+" is the value for "+ valueCount.get(value) + " entries");

		System.out.println("currentTxId = "+currentTxId);
		System.out.println("pending transactions");
		for (Integer txId : transactionLog.keySet()) {
			System.out.println("\t transaction #"+txId+" contains:");
			for (Triplet<String,String,String> trip : transactionLog.get(txId))
				System.out.println("\t\t"+trip.getValue0()+" ==>> old:"+trip.getValue1() + ", new:"+trip.getValue2());
		}
		
	}
	
	/**
	 * utility exception to clean up the handling of input errors
	 * @author ronbuchanan
	 */
	public class BadInput extends Exception {
		private static final long serialVersionUID = -9124882605241878066L;
		public BadInput(String msg){
			super(msg);
		}
	}
}
