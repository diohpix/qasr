package com.qasr.protocol.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedTransaction {
	List<TransactionObject> txs = null;
	public DistributedTransaction(){
		this.txs = new ArrayList<TransactionObject>();
	}
	public void addTransaction(TransactionObject tx){
		tx.setDistributed(true);
		this.txs.add(tx);
	}
	public void rollback() throws IOException{
		for (TransactionObject tx : this.txs) {
			tx.rollback();
		}
	}
	public void commit() throws IOException{
		for (TransactionObject tx : this.txs) {
			tx.commit();
		}
	}
}
