package ch.epfl.dias.ops.vector;

import java.util.Arrays;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	private Store store;
	
	private int vectorsize;
	private int lineNumberBegin;
	
	private int nbColumns;
	private DBColumn[] allColumns;

	public Scan(Store store, int vectorsize) {
		this.store = store;
		this.vectorsize = vectorsize;
	}
	
	@Override
	public void open() {
		lineNumberBegin = 0;
		allColumns = store.getColumns(null);
		nbColumns = allColumns.length;
		if(allColumns[0].tuples.length < vectorsize) {
			vectorsize = allColumns[0].tuples.length;
		}
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] next = new DBColumn[nbColumns];
		if(lineNumberBegin > allColumns[0].tuples.length-1) {
			return new DBColumn[] {new DBColumn(null, allColumns[0].type)};
		}
		int lineNumberEnd = lineNumberBegin + vectorsize;
		if(lineNumberEnd > allColumns[0].tuples.length-1) {
			for(int i = 0; i < nbColumns; i++) {
				next[i] = new DBColumn(Arrays.copyOfRange(allColumns[i].tuples, lineNumberBegin, allColumns[0].tuples.length), allColumns[i].type);
			}
		}else {
			for(int i = 0; i < nbColumns; i++) {
				next[i] = new DBColumn(Arrays.copyOfRange(allColumns[i].tuples, lineNumberBegin, lineNumberEnd), allColumns[i].type);
			}
		}
		lineNumberBegin += vectorsize;
		return next;
	}

	@Override
	public void close() {
		lineNumberBegin = 0;
		allColumns = null;
	}
}
