package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	private ColumnarOperator leftChild;
	private ColumnarOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		DBColumn[] leftChildColumns = leftChild.execute();
		DBColumn[] rightChildColumns = rightChild.execute();
		DBColumn leftColumn = leftChildColumns[leftFieldNo];
		DBColumn rightColumn = rightChildColumns[rightFieldNo];
		
		ArrayList<ArrayList<Object>> listColumns = new ArrayList<>();
		int tupleSize = leftChildColumns.length + rightChildColumns.length-1;
		for(int i = 0; i < tupleSize; i++){
			listColumns.add(new ArrayList<>());
		}
		testCompatibleTypes(leftColumn.type, rightColumn.type);
		
		for (int i = 0; i < leftColumn.tuples.length; i++) {
			for (int j = 0; j < rightColumn.tuples.length; j++) {
				if(isEqualJoin(leftColumn.tuples[i], leftColumn.type, rightColumn.tuples[j], rightColumn.type)) {
				//if(leftColumn.tuples[i].equals(rightColumn.tuples[j])){
					int currentPos = 0;
					for (int k = 0; k < leftChildColumns.length; k++) {
						listColumns.get(currentPos).add(leftChildColumns[k].tuples[i]);
						++currentPos;
					}
					for (int k = 0; k < rightChildColumns.length-1; k++) {
						if(k >= rightFieldNo) {
							listColumns.get(currentPos).add(rightChildColumns[k+1].tuples[j]);
						}else {
							listColumns.get(currentPos).add(rightChildColumns[k].tuples[j]);
						}
						++currentPos;
					}
				}
			}
		}
		DBColumn[] executeCol = new DBColumn[tupleSize];
		/*for(int i = 0; i < tupleSize; i++){
			executeCol[i] = new DBColumn(listColumns.get(i).toArray(), childColumns[i].type);
		}*/
		int currentPos = 0;
		for (int k = 0; k < leftChildColumns.length; k++) {
			executeCol[currentPos] = new DBColumn(listColumns.get(currentPos).toArray(), leftChildColumns[k].type);
			++currentPos;
		}
		for (int k = 0; k < rightChildColumns.length-1; k++) {
			if(k >= rightFieldNo) {
				executeCol[currentPos] = new DBColumn(listColumns.get(currentPos).toArray(), rightChildColumns[k+1].type);
			}else {
				executeCol[currentPos] = new DBColumn(listColumns.get(currentPos).toArray(), rightChildColumns[k].type);
			}
			++currentPos;
		}
		return executeCol;
	}
	
	private void testCompatibleTypes(DataType left, DataType right) {
		switch(left) {
		case INT:
		case DOUBLE:
			if(!(right == DataType.INT || right == DataType.DOUBLE)) {
				throw new RuntimeException("Join comparison with uncompatible types");
			}
			break;
		case BOOLEAN:
			if(right != DataType.BOOLEAN) {
				throw new RuntimeException("Join comparison with uncompatible types");
			}
			break;
		case STRING:
			if(right != DataType.STRING) {
				throw new RuntimeException("Join comparison with uncompatible types");
			}
			break;
		}
	}
	
	private boolean isEqualJoin(Object left, DataType leftType, Object right, DataType rightType) {
		if(leftType == DataType.INT && rightType == DataType.DOUBLE) {
			if( ((Integer) left).intValue() == ((Double) right).doubleValue()) {
				return true;
			}
		}else if(leftType == DataType.DOUBLE && rightType == DataType.INT) {
			if(((Double) left).doubleValue() == ((Integer) right).intValue()) {
				return true;
			}
		}
		else if(left.equals(right)) {
			return true;
		}
		return false;
	}
}
