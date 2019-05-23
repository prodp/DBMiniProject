package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] currentColumns = child.next();
		if(currentColumns.length == 1 && currentColumns[0].tuples == null) {
			return currentColumns;
		}
		DBColumn[] nextCol = new DBColumn[fieldNo.length];
		for(int i = 0; i < fieldNo.length; i++) {
			nextCol[i] = currentColumns[fieldNo[i]];
		}
		return nextCol;
	}

	@Override
	public void close() {
		child.close();
	}
}
