package ch.epfl.dias;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import ch.epfl.dias.store.Store;

public class Main {

	public static void main(String[] args) throws IOException {
				
		// ***************************** TEST STORES *********************************************** //

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
		rowstore.load();
		DBTuple dbtuple = rowstore.getRow(9);
		//System.out.println(dbtuple.getFieldAsString(8));

		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();
		DBColumn[] tab = columnstoreData.getColumns(new int[]{0,3});
		for(int i = 0; i < tab.length; i++) {
			Integer[] ints = tab[i].getAsInteger();
			for(int j = 0; j < ints.length; j++) {
				//System.out.println(ints[j]);
			}
		}
		
		PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		paxstore.load();
		DBTuple dbtuple_pax = paxstore.getRow(9);
		
		// ************************* END TEST STORES *********************************************** //

		// ************************ TEST EXECUTION TIME ********************************************** //
		DataType[] schemaLineitemBig = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, 
				DataType.INT, DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, 
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING};		
		DataType[] schemaOrderBig = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, 
				DataType.DOUBLE, DataType.STRING, DataType.STRING, 
				DataType.STRING, DataType.INT, DataType.STRING };
		
		int nbQuery = 6;

		//RowStore rowstoreLineitemBig = new RowStore(schemaLineitemBig, "input/lineitem_big.csv", "\\|");
		RowStore rowstoreLineitemBig = new RowStore(schemaLineitemBig, "input/lineitem_middle.csv", "\\|");
		//RowStore rowstoreOrderBig = new RowStore(schemaOrderBig, "input/orders_big.csv", "\\|");
		RowStore rowstoreOrderBig = new RowStore(schemaOrderBig, "input/orders_middle.csv", "\\|");

		rowstoreLineitemBig.load();
		rowstoreOrderBig.load();

		long startTime;

		System.out.println("\nForRowstore");
		long[] durationsRowstore = new long[nbQuery];		
		for(int i=0; i<nbQuery; i++) {
			System.out.println("Round: " + i);
			startTime = System.nanoTime();
			testRowPaxstore(rowstoreLineitemBig, rowstoreOrderBig, i);
			durationsRowstore[i] = System.nanoTime() - startTime;
			System.out.println(durationsRowstore[i] * 1.E-6 + " (miliseconds)");
		}
		System.out.println("*****************************************");
		
		//PAXStore paxstoreLineitemBig = new PAXStore(schemaLineitemBig, "input/lineitem_big.csv", "\\|", 3);
		PAXStore paxstoreLineitemBig = new PAXStore(schemaLineitemBig, "input/lineitem_middle.csv", "\\|", 3);
		//PAXStore paxstoreOrderBig = new PAXStore(schemaOrderBig, "input/orders_big.csv", "\\|", 3);
		PAXStore paxstoreOrderBig = new PAXStore(schemaOrderBig, "input/orders_middle.csv", "\\|", 3);


		paxstoreLineitemBig.load();
		paxstoreOrderBig.load();

		System.out.println("\nForPAXstore");
		//long[] durationsRowstore = new long[nbQuery];
		for(int i=0; i<nbQuery; i++) {
			System.out.println("Round: " + i);
			startTime = System.nanoTime();
			testRowPaxstore(paxstoreLineitemBig, paxstoreOrderBig, i);
			durationsRowstore[i] = System.nanoTime() - startTime;
			System.out.println(durationsRowstore[i] * 1.E-6 + " (miliseconds)");
		}
		System.out.println("*****************************************");


		System.out.println("\nForColumnstore_Column");
		//ColumnStore columnstoreLineitemBig = new ColumnStore(schemaLineitemBig, "input/lineitem_big.csv", "\\|");
		ColumnStore columnstoreLineitemBig = new ColumnStore(schemaLineitemBig, "input/lineitem_middle.csv", "\\|");
		//ColumnStore columnstoreOrderBig =  new ColumnStore(schemaOrderBig, "input/orders_big.csv", "\\|");
		ColumnStore columnstoreOrderBig =  new ColumnStore(schemaOrderBig, "input/orders_middle.csv", "\\|");

		columnstoreLineitemBig.load();
		columnstoreOrderBig.load();

		long[] durationsColumn = new long[nbQuery];		
		for(int i=0; i<nbQuery; i++) {
			System.out.println("Round: " + i);
			startTime = System.nanoTime();
			testColumnstore_column(columnstoreLineitemBig, columnstoreOrderBig, i);
			durationsColumn[i] = System.nanoTime() - startTime;
			System.out.println(durationsColumn[i] * 1.E-6 + " (miliseconds)");
		};
		
		System.out.println("\nForColumnstore_Vector");
		for(int i=0; i<nbQuery; i++) {
			System.out.println("Round: " + i);
			startTime = System.nanoTime();
			testColumnstore_vector(columnstoreLineitemBig, columnstoreOrderBig, i);
			durationsColumn[i] = System.nanoTime() - startTime;
			System.out.println(durationsColumn[i] * 1.E-6 + " (miliseconds)");
		};
		
		System.out.println("*****************************************");



	}//end of main method


	private static void testRowPaxstore(Store storeLine, Store storeOrder, int queryNum) {
		switch(queryNum) {
			case 0:
				/* SELECT * FROM lineitem WHERE col1 >= 156345 */        
				ch.epfl.dias.ops.volcano.Scan scan0 = new ch.epfl.dias.ops.volcano.Scan(storeLine);
				ch.epfl.dias.ops.volcano.Select sel0 = new ch.epfl.dias.ops.volcano.Select(scan0, BinaryOp.GE, 1, 156345);
				sel0.open();
				DBTuple curr0 = sel0.next();
				while(!curr0.eof) {
					curr0 = sel0.next();
				}
				break;
			case 1:
				/* SELECT col1, col2 FROM lineitem */        
				ch.epfl.dias.ops.volcano.Scan scan1 = new ch.epfl.dias.ops.volcano.Scan(storeLine);
				ch.epfl.dias.ops.volcano.Project proj1 = new ch.epfl.dias.ops.volcano.Project(scan1, new int[]{0,1});
				proj1.open();
				DBTuple curr1 = proj1.next();
				while(!curr1.eof) {
					curr1 = proj1.next();
				}
				break;
			case 2:
				/* SELECT col1, col2 FROM lineitem WHERE col1 >= 156345 */        
				ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(storeLine);
				ch.epfl.dias.ops.volcano.Select sel2 = new ch.epfl.dias.ops.volcano.Select(scan2, BinaryOp.GE, 1, 156345);
				ch.epfl.dias.ops.volcano.Project proj2 = new ch.epfl.dias.ops.volcano.Project(sel2, new int[]{0,1});
				proj2.open();
				DBTuple curr2 = proj2.next();
				while(!curr2.eof) {
					curr2 = proj2.next();
				}
				break;
			case 3:
				/*SELECT l.attri, o.attrj, ...
			            	FROM lineitem l, orders o
			            	WHERE l.l_orderkey=o.o_orderkey*/            	
				ch.epfl.dias.ops.volcano.Scan scan30 = new ch.epfl.dias.ops.volcano.Scan(storeLine);
				ch.epfl.dias.ops.volcano.Scan scan31 = new ch.epfl.dias.ops.volcano.Scan(storeOrder);
				ch.epfl.dias.ops.volcano.HashJoin join3 = new ch.epfl.dias.ops.volcano.HashJoin(scan30,scan31,0,0);
				ch.epfl.dias.ops.volcano.Project proj3 = new ch.epfl.dias.ops.volcano.Project(join3, new int[]{0,16});
				proj3.open();
				DBTuple curr3 = proj3.next();
				while(!curr3.eof) {
					curr3 = proj3.next();
				}
				break;
			case 4:
				/*SELECT AGGR(attri)
			            	FROM lineitem */
				ch.epfl.dias.ops.volcano.Scan scan4 = new ch.epfl.dias.ops.volcano.Scan(storeLine);
				ch.epfl.dias.ops.volcano.ProjectAggregate agg4 = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan4, Aggregate.SUM, DataType.INT, 0);            	
				agg4.open();
				agg4.next();
				break;
			case 5:
				/*SELECT AGGR(o.attri)
			            	FROM lineitem l, orders o
			            	WHERE l.l_orderkey=o.o_orderkey*/
				ch.epfl.dias.ops.volcano.Scan scan50 = new ch.epfl.dias.ops.volcano.Scan(storeLine);
				ch.epfl.dias.ops.volcano.Scan scan51 = new ch.epfl.dias.ops.volcano.Scan(storeOrder);
				ch.epfl.dias.ops.volcano.HashJoin join5 = new ch.epfl.dias.ops.volcano.HashJoin(scan50,scan51,0,0);
				ch.epfl.dias.ops.volcano.ProjectAggregate agg5 = new ch.epfl.dias.ops.volcano.ProjectAggregate(join5, Aggregate.SUM, DataType.INT, 0);           	
				agg5.open();
				agg5.next();
				break;
		}
	}
	
	private static void testColumnstore_column(ColumnStore columnStoreLine, ColumnStore columnStoreOrder, int queryNum) {
		switch(queryNum) {
		case 0:
			/* SELECT * FROM lineitem WHERE col1 >= 156345 */        
			ch.epfl.dias.ops.columnar.Scan scan0 = new ch.epfl.dias.ops.columnar.Scan(columnStoreLine);
			ch.epfl.dias.ops.columnar.Select sel0 = new ch.epfl.dias.ops.columnar.Select(scan0, BinaryOp.GE, 1, 156345);
			DBColumn[] curr0 = sel0.execute();
			//Integer[] int0 = curr0[0].getAsInteger();
			break;
		case 1:
			/* SELECT col1, col2 FROM lineitem*/        
			ch.epfl.dias.ops.columnar.Scan scan1 = new ch.epfl.dias.ops.columnar.Scan(columnStoreLine);
			ch.epfl.dias.ops.columnar.Project proj1 = new ch.epfl.dias.ops.columnar.Project(scan1, new int[]{0,1});
			DBColumn[] curr1 = proj1.execute();
			//Integer[] int1 = curr1[0].getAsInteger();
			break;
		case 2:
			/* SELECT col1, col2 FROM lineitem WHERE col1 >= 156345 */        
			ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnStoreLine);
			ch.epfl.dias.ops.columnar.Select sel2 = new ch.epfl.dias.ops.columnar.Select(scan2, BinaryOp.GE, 1, 156345);
			ch.epfl.dias.ops.columnar.Project proj2 = new ch.epfl.dias.ops.columnar.Project(sel2, new int[]{0,1});
			DBColumn[] curr2 = proj2.execute();
			//Integer[] int2 = curr2[0].getAsInteger();
			break;
		case 3:
			/*SELECT l.attri, o.attrj, ...
		            	FROM lineitem l, orders o
		            	WHERE l.l_orderkey=o.o_orderkey*/            	
			ch.epfl.dias.ops.columnar.Scan scan30 = new ch.epfl.dias.ops.columnar.Scan(columnStoreLine);
			ch.epfl.dias.ops.columnar.Scan scan31 = new ch.epfl.dias.ops.columnar.Scan(columnStoreOrder);
			ch.epfl.dias.ops.columnar.Join join3 = new ch.epfl.dias.ops.columnar.Join(scan30,scan31,0,0);
			ch.epfl.dias.ops.columnar.Project proj3 = new ch.epfl.dias.ops.columnar.Project(join3, new int[]{0,16});
			DBColumn[] curr3 = proj3.execute();
			//Integer[] int3 = curr3[0].getAsInteger();
			break;
		case 4:
			/*SELECT AGGR(attri)
        		FROM lineitem */
			ch.epfl.dias.ops.columnar.Scan scan4 = new ch.epfl.dias.ops.columnar.Scan(columnStoreLine);
			ch.epfl.dias.ops.columnar.ProjectAggregate agg4 = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan4, Aggregate.SUM, DataType.INT, 0);            	
			DBColumn[] result4 = agg4.execute();
			//int output4 = result4[0].getAsInteger()[0];
			break;
		case 5:
			/*SELECT AGGR(o.attri)
		            	FROM lineitem l, orders o
		            	WHERE l.l_orderkey=o.o_orderkey*/
			ch.epfl.dias.ops.columnar.Scan scan50 = new ch.epfl.dias.ops.columnar.Scan(columnStoreLine);
			ch.epfl.dias.ops.columnar.Scan scan51 = new ch.epfl.dias.ops.columnar.Scan(columnStoreOrder);
			ch.epfl.dias.ops.columnar.Join join5 = new ch.epfl.dias.ops.columnar.Join(scan50,scan51,0,0);
			ch.epfl.dias.ops.columnar.ProjectAggregate agg5 = new ch.epfl.dias.ops.columnar.ProjectAggregate(join5, Aggregate.SUM, DataType.INT, 0);           	
			DBColumn[] result5 = agg5.execute();
			//int output5 = result5[0].getAsInteger()[0];
			break;
		}
	}
	
	private static void testColumnstore_vector(ColumnStore columnStoreLine, ColumnStore columnStoreOrder, int queryNum) {
		int vectorsize = 3;
		switch(queryNum) {
			case 0:
				/* SELECT * FROM lineitem WHERE col1 >= 156345 */        
				ch.epfl.dias.ops.vector.Scan scan0 = new ch.epfl.dias.ops.vector.Scan(columnStoreLine, vectorsize);
				ch.epfl.dias.ops.vector.Select sel0 = new ch.epfl.dias.ops.vector.Select(scan0, BinaryOp.GE, 1, 156345);
				sel0.open();
				DBColumn[] curr0 = sel0.next();
				while(curr0[0].tuples != null) {
					curr0 = sel0.next();
				}
				break;
			case 1:
				/* SELECT col1, col2 FROM lineitem*/        
				ch.epfl.dias.ops.vector.Scan scan1 = new ch.epfl.dias.ops.vector.Scan(columnStoreLine, vectorsize);
				ch.epfl.dias.ops.vector.Project proj1 = new ch.epfl.dias.ops.vector.Project(scan1, new int[]{0,1});
				proj1.open();
				DBColumn[] curr1 = proj1.next();
				while(curr1[0].tuples != null) {
					curr1 = proj1.next();
				}
				break;
			case 2:
				/* SELECT col1, col2 FROM lineitem WHERE col1 >= 156345 */        
				ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnStoreLine, vectorsize);
				ch.epfl.dias.ops.vector.Select sel2 = new ch.epfl.dias.ops.vector.Select(scan2, BinaryOp.GE, 1, 156345);
				ch.epfl.dias.ops.vector.Project proj2 = new ch.epfl.dias.ops.vector.Project(sel2, new int[]{0,1});
				proj2.open();
				DBColumn[] curr2 = proj2.next();
				while(curr2[0].tuples != null) {
					curr2 = proj2.next();
				}
				break;
			case 3:
				/*SELECT l.attri, o.attrj, ...
			            	FROM lineitem l, orders o
			            	WHERE l.l_orderkey=o.o_orderkey*/            	
				ch.epfl.dias.ops.vector.Scan scan30 = new ch.epfl.dias.ops.vector.Scan(columnStoreLine, vectorsize);
				ch.epfl.dias.ops.vector.Scan scan31 = new ch.epfl.dias.ops.vector.Scan(columnStoreOrder, vectorsize);
				ch.epfl.dias.ops.vector.Join join3 = new ch.epfl.dias.ops.vector.Join(scan30,scan31,0,0);
				ch.epfl.dias.ops.vector.Project proj3 = new ch.epfl.dias.ops.vector.Project(join3, new int[]{0,16});
				proj3.open();
				DBColumn[] curr3 = proj3.next();
				while(curr3[0].tuples != null) {
					curr3 = proj3.next();
				}
				break;
			case 4:
				/*SELECT AGGR(attri)
        			FROM lineitem */
				ch.epfl.dias.ops.vector.Scan scan4 = new ch.epfl.dias.ops.vector.Scan(columnStoreLine, vectorsize);
				ch.epfl.dias.ops.vector.ProjectAggregate agg4 = new ch.epfl.dias.ops.vector.ProjectAggregate(scan4, Aggregate.SUM, DataType.INT, 0);            	
				agg4.open();
				agg4.next();
				//System.out.println(agg4.next()[0].getAsInteger()[0]);
				break;
			case 5:
				/*SELECT AGGR(o.attri)
			            	FROM lineitem l, orders o
			            	WHERE l.l_orderkey=o.o_orderkey*/
				ch.epfl.dias.ops.vector.Scan scan50 = new ch.epfl.dias.ops.vector.Scan(columnStoreLine, vectorsize);
				ch.epfl.dias.ops.vector.Scan scan51 = new ch.epfl.dias.ops.vector.Scan(columnStoreOrder, vectorsize);
				ch.epfl.dias.ops.vector.Join join5 = new ch.epfl.dias.ops.vector.Join(scan50,scan51,0,0);
				ch.epfl.dias.ops.vector.ProjectAggregate agg5 = new ch.epfl.dias.ops.vector.ProjectAggregate(join5, Aggregate.SUM, DataType.INT, 0);           	
				agg5.open();
				agg5.next();
				//System.out.println(agg5.next()[0].getAsInteger()[0]);
				break;
		}
	}
}
