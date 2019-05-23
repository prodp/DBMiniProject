package ch.epfl.dias.ops.vector;

import java.util.ArrayList;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	
	private boolean firstTime;
	private int vectorsize;
	private int nbColumns;
	private boolean eof;
	
	private ArrayList<ArrayList<Object>> remainingColumns = new ArrayList<>();
	private DataType[] types;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}
	
	@Override
	public void open() {
		child.open();
		firstTime = true;
		eof = false;
	}

	@Override
	public DBColumn[] next() {
		if(eof) {
			//child will return null tuples
			if(remainingColumns.get(0).size() != 0) {
				DBColumn[] n = new DBColumn[remainingColumns.size()];
				for(int i = 0; i < remainingColumns.size(); i++){
					n[i] = new DBColumn(remainingColumns.get(i).toArray(), types[i]);
				}
				return n;
			}
			return child.next();
		}
		DBColumn[] currentColumns = child.next();
		DBColumn[] next;
		if(firstTime) {
			nbColumns = currentColumns.length;
			vectorsize = currentColumns[0].tuples.length;
			types = new DataType[nbColumns];
			for(int i = 0; i < nbColumns; i++){
				remainingColumns.add(new ArrayList<>());
				types[i] = currentColumns[i].type;
			}
			firstTime = false;
		}
		ArrayList<ArrayList<Object>> listColumns = new ArrayList<>();
		for(int i = 0; i < nbColumns; i++){
			listColumns.add(remainingColumns.get(i));
			remainingColumns.set(i, new ArrayList<>());
		}
		
		if(currentColumns[0].tuples == null) {
			eof = true;
			if(listColumns.get(0).size() != 0) {
				DBColumn[] n = new DBColumn[listColumns.size()];
				for(int i = 0; i < listColumns.size(); i++){
					n[i] = new DBColumn(listColumns.get(i).toArray(), types[i]);
				}
				return n;
			}
			return currentColumns;
		}
		while(!eof) {
			if(currentColumns[0].tuples.length < vectorsize) {
				eof = true;
			}
			Object[] selectColumn = currentColumns[fieldNo].tuples;
			
			for (int i = 0; i < selectColumn.length; i++) {
				if(op.apply((Integer) selectColumn[i], value)) {
					if(listColumns.get(0).size() >= vectorsize) {
						for (int j = 0; j < nbColumns; j++) {
							remainingColumns.get(j).add(currentColumns[j].tuples[i]);
						}
					}else {
						for (int j = 0; j < nbColumns; j++) {
							listColumns.get(j).add(currentColumns[j].tuples[i]);
						}
					}
				}
			}
			if(listColumns.get(0).size() >= vectorsize) {
				break;
			}
			next = child.next();
			if(next[0].tuples == null) {
				eof = true;
			}else {
				currentColumns = next;
			}
		}
		
		DBColumn[] nextCols = new DBColumn[nbColumns];
		for(int i = 0; i < nbColumns; i++){
			nextCols[i] = new DBColumn(listColumns.get(i).toArray(), types[i]);
		}
		return nextCols;
	}

	@Override
	public void close() {
		child.close();
		firstTime = true;
		eof = false;
	}
}
