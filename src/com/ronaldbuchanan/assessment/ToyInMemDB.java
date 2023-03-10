package com.ronaldbuchanan.assessment;

import java.util.HashMap;
import java.util.ArrayDeque;

import org.javatuples.Triplet;

public class ToyInMemDB {
	/* 
	 * Principal data structures:
	 * 
	 * 		database - a simple key-value map 
	 * 
	 * 		valueCount - contains the counts of the values
	 * 
	 * 		transactionLog - maintains multiple stacks of the outstanding transactions
	 * 
	 *  	currentTxId - index of the current transaction (more efficient than checking the 
	 *  				  maximum value the transactionLog's keySet)
	 *  
	 *  NOTE: 
	 *  	Should there ever be a need to implement conditioned get() operations ("where" clauses), an alternate
	 *  	implementation would be to replace valueCount with an index:
	 *  		HashMap<String,HashSet<String>> index = new HashMap<>()
	 *  	
	 */
	
	private final HashMap<String,String> database = new HashMap<>();
	private final HashMap<String,Integer> valueCount = new HashMap<>();
	private final HashMap<Integer,ArrayDeque<Triplet<String,String,String>>> transactionLog = new HashMap<>();
	private Integer currentTxId = 0;
	
	public ToyInMemDB() {
	}
	
	/**
	 * set the value for a name
	 * @param name
	 * @param value
	 */
	public void set(String name, String value) {
		String oldValue = database.getOrDefault(name, null);
		
		if (value.equals(oldValue))
			return; //no change, it's a push
		
		//log to the transaction log
		if (currentTxId>0)
			transactionLog.get(currentTxId).addLast(Triplet.with(name, oldValue, value));
		
		//update the database
		database.put(name, value);
		
		//update the index
		if (oldValue!=null)
			valueCount.put(oldValue, valueCount.getOrDefault(oldValue, 0)-1);
		valueCount.put(value, 1+valueCount.getOrDefault(value, 0));
	}
	
	/**
	 * delete a name
	 * @param name
	 */
	public void delete(String name) {
		if (database.containsKey(name)) {
			String oldValue = database.get(name);
			String newValue = null;
			
			//log to the transaction log
			if (currentTxId>0)
				transactionLog.get(currentTxId).addLast(Triplet.with(name, oldValue, newValue));
			
			//update the index
			valueCount.put(oldValue, valueCount.get(oldValue)-1);
			
			//update the database
			database.remove(name);
		}
	}
	
	/**
	 * get the value for a name
	 * @param name
	 */
	public String get(String name) {
		return database.getOrDefault(name,"NULL");
	}
	
	/**
	 * get the number of occurrences of a value
	 */
	public int count(String value) {
		return valueCount.getOrDefault(value, 0);
	}
	
	/**
	 * start a new transaction
	 */
	public void begin() {
		//increment the current transaction id and insert new stack for the new transaction 
		transactionLog.put(++currentTxId, new ArrayDeque<>());
	}
	
	/**
	 * rollback the current transaction
	 */
	public void rollback() {
		if (currentTxId==0) {
			System.out.println("TRANSACTION NOT FOUND");
			return;
		}

		ArrayDeque<Triplet<String,String,String>> tx = transactionLog.get(currentTxId);
		while (!tx.isEmpty()) { // definitely O(n)
			Triplet<String,String,String> entry = tx.pollLast();
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
			if (oldValue!=null)
				valueCount.put(oldValue, 1 + valueCount.getOrDefault(oldValue, 0));
		}
		
		//remove the entries from the current transaction and decrement the current transaction id
		transactionLog.remove(currentTxId--);
	}
	
	/**
	 * commit all outstanding transactions
	 */
	public void commit() {
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
	private static class BadInput extends Exception {
		private static final long serialVersionUID = -9124882605241878066L;
		public BadInput(String msg){
			super(msg);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting ... ");

		boolean debug = args.length>0 && "-debug".equals(args[0].toLowerCase());
		ToyInMemDB db = new ToyInMemDB();
		
		java.io.Console console = System.console();
		while(true) {
			String sysin = console.readLine(">> ");
			if (sysin==null || sysin.trim().length()==0)
				continue;
			
			String[] input = sysin.split(" ");
			try {
				switch (input[0].toUpperCase()) {
				case "SET": 		
					{
						if (input.length!=3)
							throw new BadInput("improper command: SET accepts 2 parameters: [name] and [value]");

						String name = input[1];
						String value = input[2];
						db.set(name, value);							
					}
					break;
				case "GET":
					{
						if (input.length!=2)
							throw new BadInput("improper command: GET accepts 1 parameter: [name]");
						String name = input[1];
						String value = db.get(name);
						System.out.println(value);	
					}
					break;
				case "DELETE": 
					{		
						if (input.length!=2)
							throw new BadInput("improper command: DELETE accepts 1 parameter: [name]");

						String name = input[1];
						db.delete(name);
					}
					break;
				case "COUNT":	
					{
						if (input.length!=2)
							throw new BadInput("improper command: COUNT accepts 1 parameter: [value]");
						
						String value = input[1];
						int count = db.count(value);
						System.out.println(count);	
					}
					break;
				case "BEGIN":	
					{
						if (input.length!=1)
							throw new BadInput("improper command: BEGIN does not accept any parameters");
						db.begin(); 	
					}
					break;
				case "ROLLBACK":
					{
						if (input.length!=1)
							throw new BadInput("improper command: ROLLBACK does not accept any parameters");
						db.rollback();			
					}
					break;
				case "COMMIT":
					{
						if (input.length!=1)
							throw new BadInput("improper command: COMMIT does not accept any parameters");
						db.commit();
					}
					break;
				case "END": 
					System.out.println("\nsession complete, terminating ..."); 
					return;
				case "DUMP":		
					if (debug) { db.dump(); break; }
				default:
					System.out.println("unrecognized function: " + input[0]);
				}
			} catch (BadInput e) {
				System.err.print(e.getMessage());
			}
		}
	}
}
