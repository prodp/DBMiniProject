package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	private ColumnarOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] childColumns = child.execute();
		if(childColumns.length == 0 || childColumns[fieldNo].tuples.length == 0) {
			return new DBColumn[]{};
		}
		if(childColumns[fieldNo].type != DataType.INT) {
			throw new RuntimeException("Select on a column that hasn't type Integer");
		}
		Integer[] selectColumn = childColumns[fieldNo].getAsInteger();
		ArrayList<ArrayList<Object>> listColumns = new ArrayList<>();
		for(int i = 0; i < childColumns.length; i++){
			listColumns.add(new ArrayList<>());
		}
		for (int i = 0; i < selectColumn.length; i++) {
			if(op.apply(selectColumn[i], value)) {
				for (int j = 0; j < childColumns.length; j++) {
					listColumns.get(j).add(childColumns[j].tuples[i]);
				}
			}
		}
		DBColumn[] executeCol = new DBColumn[childColumns.length];
		for(int i = 0; i < childColumns.length; i++){
			executeCol[i] = new DBColumn(listColumns.get(i).toArray(), childColumns[i].type);
		}
		return executeCol;
	}
}
