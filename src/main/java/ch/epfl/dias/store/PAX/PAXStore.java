package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int tuplesPerPage;
	private int nbTuples;
	
	private ArrayList<DBPAXpage> paxStore = new ArrayList<>();

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = new DataType[schema.length];
		System.arraycopy(schema, 0, this.schema, 0, schema.length);
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		this.nbTuples = 0;
	}

	@Override
	public void load() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = "";
		Object[][] data = new Object[schema.length][tuplesPerPage];
		int nbTupleCurrOnPage = 0;
		while( (line = br.readLine()) != null){
			nbTuples++;
			String[] strFields = line.split(delimiter);
			for (int i = 0; i < strFields.length; i++) {
				switch(schema[i]) {
					case INT:
						data[i][nbTupleCurrOnPage] = Integer.parseInt(strFields[i]);
						break;
					case DOUBLE:
						data[i][nbTupleCurrOnPage] = Double.parseDouble(strFields[i]);
						break;
					case BOOLEAN:
						data[i][nbTupleCurrOnPage] = Boolean.parseBoolean(strFields[i]);
						break;
					case STRING:
						data[i][nbTupleCurrOnPage] = strFields[i];
						break;
					default:
						throw new RuntimeException("Type doesn't match any DataType");
				}
			}
			nbTupleCurrOnPage++;
			if (nbTupleCurrOnPage == tuplesPerPage) {
				DBPAXpage page = new DBPAXpage(data, schema);
				paxStore.add(page);
				nbTupleCurrOnPage = 0;
				data = new Object[schema.length][tuplesPerPage];
			}
		}
		if (nbTupleCurrOnPage != tuplesPerPage) {
			DBPAXpage page = new DBPAXpage(data, schema);
			paxStore.add(page);
		}
		br.close();
	}

	@Override
	public DBTuple getRow(int rownumber) {
		if (rownumber >= nbTuples) {
			return new DBTuple();
		}
		Object[] fields = new Object[schema.length];
		int pagenumber = rownumber / tuplesPerPage;
		DBPAXpage currPage = paxStore.get(pagenumber);
		int indexOnPage = rownumber % tuplesPerPage;
		for (int i = 0; i < schema.length; i++) {
			fields[i] = currPage.data[i][indexOnPage];
		}
		return new DBTuple(fields, schema);
	}
}
