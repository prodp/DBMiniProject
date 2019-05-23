package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	
	private DBColumn[] currLeft;
	private int vectorsize;
	
	private ArrayList<ArrayList<Object>> remainingColumns = new ArrayList<>();
	private DataType[] types;
	private int tupleSize;
	private boolean firstTime;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		this.currLeft = leftChild.next();
		this.vectorsize = currLeft[0].tuples.length;
		firstTime = true;
	}

	@Override
	public DBColumn[] next() {
		if(this.currLeft[0].tuples == null) {
			if(remainingColumns.get(0).size() != 0) {
				DBColumn[] n = new DBColumn[remainingColumns.size()];
				for(int i = 0; i < remainingColumns.size(); i++){
					n[i] = new DBColumn(remainingColumns.get(i).toArray(), types[i]);
				}
				return n;
			}
			return currLeft;
		}
		
		DBColumn[] currRight;
		ArrayList<ArrayList<Object>> listColumns = new ArrayList<>();
		
		boolean cond = true;
		do {
			currRight = rightChild.next();			
			if(currRight[0].tuples == null) {
				this.currLeft = leftChild.next();
				if(this.currLeft[0].tuples == null) {
					return new DBColumn[] {new DBColumn(null, null)};
				}
				rightChild.close();
				rightChild.open();
				currRight = rightChild.next();
			}
			
			if(firstTime) {
				// right est vide
				if(currRight[0].tuples == null) {
					return currRight;
				}
				tupleSize = currLeft.length + currRight.length-1;
				for(int i = 0; i < tupleSize; i++){
					remainingColumns.add(new ArrayList<>());
					listColumns.add(new ArrayList<>());
				}
				types = new DataType[tupleSize];
				int currentPos = 0;
				for (int k = 0; k < currLeft.length; k++) {
					types[currentPos] = currLeft[k].type;
					++currentPos;
				}
				for (int k = 0; k < currRight.length-1; k++) {
					if(k >= rightFieldNo) {
						types[currentPos] = currRight[k+1].type;
					}else {
						types[currentPos] = currRight[k].type;
					}
					++currentPos;
				}
				firstTime = false;
			}else {
				for(int i = 0; i < tupleSize; i++){
					listColumns.add(remainingColumns.get(i));
					remainingColumns.set(i, new ArrayList<>());
				}
			}
			
			DataType leftType = currLeft[leftFieldNo].type;
			DataType rightType = currRight[rightFieldNo].type;
			if( leftType != rightType && 
					!((leftType == DataType.INT || leftType == DataType.DOUBLE) && 
					rightType == DataType.INT || rightType == DataType.DOUBLE)) {
				throw new RuntimeException("Join comparison with uncompatible types");
			}
			if(leftType == DataType.INT && rightType == DataType.DOUBLE) {
				Integer[] leftCol = currLeft[leftFieldNo].getAsInteger();
				Double[] rightCol = currRight[rightFieldNo].getAsDouble();
				for (int i = 0; i < leftCol.length; i++) {
					for (int j = 0; j < rightCol.length; j++) {
						if(leftCol[i].intValue() == rightCol[j].doubleValue()) {
							//cond = false;
							//ajouter les éléments dans arraylist
							if(listColumns.get(0).size() >= vectorsize) {
								int currentPos = 0;
								for (int k = 0; k < currLeft.length; k++) {
									remainingColumns.get(currentPos).add(currLeft[k].tuples[i]);
									++currentPos;
								}
								for (int k = 0; k < currRight.length-1; k++) {
									if(k >= rightFieldNo) {
										remainingColumns.get(currentPos).add(currRight[k+1].tuples[j]);
									}else {
										remainingColumns.get(currentPos).add(currRight[k].tuples[j]);
									}
									++currentPos;
								}
							}else {
								int currentPos = 0;
								for (int k = 0; k < currLeft.length; k++) {
									listColumns.get(currentPos).add(currLeft[k].tuples[i]);
									++currentPos;
								}
								for (int k = 0; k < currRight.length-1; k++) {
									if(k >= rightFieldNo) {
										listColumns.get(currentPos).add(currRight[k+1].tuples[j]);
									}else {
										listColumns.get(currentPos).add(currRight[k].tuples[j]);
									}
									++currentPos;
								}
							}
							if(listColumns.get(0).size() >= vectorsize) {
								cond = false;
							}
						}
					}
				}
			}else if(leftType == DataType.DOUBLE && rightType == DataType.INT) {
				Double[] leftCol = currLeft[leftFieldNo].getAsDouble();
				Integer[] rightCol = currRight[rightFieldNo].getAsInteger();
				for (int i = 0; i < leftCol.length; i++) {
					for (int j = 0; j < rightCol.length; j++) {
						if(leftCol[i].doubleValue() == rightCol[j].intValue()) {
							cond = false;
							//ajouter les éléments dans arraylist
							if(listColumns.get(0).size() >= vectorsize) {
								int currentPos = 0;
								for (int k = 0; k < currLeft.length; k++) {
									remainingColumns.get(currentPos).add(currLeft[k].tuples[i]);
									++currentPos;
								}
								for (int k = 0; k < currRight.length-1; k++) {
									if(k >= rightFieldNo) {
										remainingColumns.get(currentPos).add(currRight[k+1].tuples[j]);
									}else {
										remainingColumns.get(currentPos).add(currRight[k].tuples[j]);
									}
									++currentPos;
								}
							}else {
								int currentPos = 0;
								for (int k = 0; k < currLeft.length; k++) {
									listColumns.get(currentPos).add(currLeft[k].tuples[i]);
									++currentPos;
								}
								for (int k = 0; k < currRight.length-1; k++) {
									if(k >= rightFieldNo) {
										listColumns.get(currentPos).add(currRight[k+1].tuples[j]);
									}else {
										listColumns.get(currentPos).add(currRight[k].tuples[j]);
									}
									++currentPos;
								}
							}
							if(listColumns.get(0).size() >= vectorsize) {
								cond = false;
							}
						}
					}
				}
			}
			else { 
				Object[] leftCol = currLeft[leftFieldNo].tuples;
				Object[] rightCol = currRight[rightFieldNo].tuples;
				for (int i = 0; i < leftCol.length; i++) {
					for (int j = 0; j < rightCol.length; j++) {
						if(leftCol[i].equals(rightCol[j])) {
							cond = false;
							//ajouter les éléments dans arraylist
							if(listColumns.get(0).size() >= vectorsize) {
								int currentPos = 0;
								for (int k = 0; k < currLeft.length; k++) {
									remainingColumns.get(currentPos).add(currLeft[k].tuples[i]);
									++currentPos;
								}
								for (int k = 0; k < currRight.length-1; k++) {
									if(k >= rightFieldNo) {
										remainingColumns.get(currentPos).add(currRight[k+1].tuples[j]);
									}else {
										remainingColumns.get(currentPos).add(currRight[k].tuples[j]);
									}
									++currentPos;
								}
							}else {
								int currentPos = 0;
								for (int k = 0; k < currLeft.length; k++) {
									listColumns.get(currentPos).add(currLeft[k].tuples[i]);
									++currentPos;
								}
								for (int k = 0; k < currRight.length-1; k++) {
									if(k >= rightFieldNo) {
										listColumns.get(currentPos).add(currRight[k+1].tuples[j]);
									}else {
										listColumns.get(currentPos).add(currRight[k].tuples[j]);
									}
									++currentPos;
								}
							}
							if(listColumns.get(0).size() >= vectorsize) {
								cond = false;
							}
							
						}
					}
				}
			}
		} while(cond);
		
		DBColumn[] executeCol = new DBColumn[tupleSize];
		int currentPos = 0;
		for (int k = 0; k < currLeft.length; k++) {
			executeCol[currentPos] = new DBColumn(listColumns.get(currentPos).toArray(), currLeft[k].type);
			++currentPos;
		}
		for (int k = 0; k < currRight.length-1; k++) {
			if(k >= rightFieldNo) {
				executeCol[currentPos] = new DBColumn(listColumns.get(currentPos).toArray(), currRight[k+1].type);
			}else {
				executeCol[currentPos] = new DBColumn(listColumns.get(currentPos).toArray(), currRight[k].type);
			}
			++currentPos;
		}
		return executeCol;
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
		this.currLeft = null;
		firstTime = true;
	}
}
