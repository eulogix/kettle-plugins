package com.eulogix.kettle.lib.ui;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.lang.ArrayUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.FieldDisabledListener;
import org.pentaho.di.ui.core.widget.TableView;

public class Grid {
	
	public TableView tableView;
	
	private LinkedHashMap<String, ColumnInfo> columnsInfo = new LinkedHashMap<String, ColumnInfo>();
	
	public Grid() {
			
	}
	
	public void addColumn(String name, ColumnInfo colInfo) {
	    this.columnsInfo.put(name, colInfo);
	}
	
	public ColumnInfo getColumnInfo(String columnName) {
		return this.columnsInfo.get(columnName);
	}
	
	public void initTableView( VariableSpace space, Composite parent, int style, int nrRows, ModifyListener lsm, PropsUI pr ) {
	    tableView =  new TableView(space, parent, style, getColumnInfoArray(), nrRows, lsm, pr);
	    
	    ModifyListener ml = new ModifyListener() {
			@Override
			public void modifyText(ModifyEvent arg0) {
				refreshTable();
			}
	    };
	    
	    getTable().addModifyListener(ml);
	    refreshTable();
	}

	public ColumnInfo[] getColumnInfoArray() {
		return columnsInfo.values().toArray(new ColumnInfo[0]);
	}

	public TableView getTable() {
		return tableView;
	}

	public int getColumnIndex(String columnName) {
		return ArrayUtils.indexOf(getColumnNames(), columnName);
	}
	
	public String getColumnName(int columnIndex) {
		return getColumnNames()[columnIndex];
	}
	
	public String[] getColumnNames() {
		return columnsInfo.keySet().toArray(new String[0]);
	}
	
	/**
	   * refreshes the table setting background colors according to the read only status of cells
	   */
	  private void refreshTable() {
		  FieldDisabledListener fdl;
		  ColumnInfo[] columnInfo = getColumnInfoArray();
		  
		  for ( int i = 0; i < getTable().table.getItemCount(); i++ ) {
			  TableItem item = getTable().table.getItem( i );
			  
			  for( int j = 1; j < getTable().table.getColumnCount(); j++) {
				  fdl = columnInfo[j-1].getDisabledListener();
				  if (fdl != null && fdl.isFieldDisabled(i)) {
					  item.setBackground(j, GUIResource.getInstance().getColorLightGray());
				  } else item.setBackground(j, GUIResource.getInstance().getColorWhite());
			  }
	      }		  
	  }

	public void setData(ArrayList<HashMap<String, String>> data) {
		getTable().table.removeAll();
		int i = 1;
		  for (HashMap<String, String> line : data) {
			TableItem item = new TableItem(getTable().table, SWT.NONE);
			item.setText(0, Integer.toString(i++));
			for (Map.Entry<String, String> entry : line.entrySet()) {
				if(entry.getValue() != null)
					item.setText( 1+(Integer) getColumnIndex( entry.getKey() ), entry.getValue().toString());
			}
		  }
		if(data.size()==0)
			getTable().removeAll(); //otherwise it gets stuck in 0 lines mode
		refreshTable();
	}

	public ArrayList<HashMap<String, String>> getData() {
		ArrayList<HashMap<String, String>> ret = new ArrayList<HashMap<String, String>>();
		
		for ( int i = 0; i < getTable().table.getItemCount(); i++ ) { //rows
			  TableItem item = getTable().table.getItem( i );
			  HashMap<String,String> rowData = new HashMap<String,String>();
			  for( int j = 1; j < getTable().table.getColumnCount(); j++) {
					  //cols
					  rowData.put(
							  getColumnName(j-1).toString()
							  , item.getText(j));
				  }
			  ret.add(rowData);
	      }
		return ret;
	}
	  
}
