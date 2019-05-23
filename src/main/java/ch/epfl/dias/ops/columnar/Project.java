package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements ColumnarOperator {

	private ColumnarOperator child;
	private int[] columns;

	public Project(ColumnarOperator child, int[] columns) {
		this.child = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		DBColumn[] childColumns = child.execute();
		if(childColumns.length == 0) {
			return new DBColumn[]{};
		}
		DBColumn[] executeCol = new DBColumn[columns.length];
		for(int i = 0; i < columns.length; i++) {
			executeCol[i] = childColumns[columns[i]];
		}
		return executeCol;
	}
}
