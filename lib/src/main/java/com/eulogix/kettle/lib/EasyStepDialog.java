package com.eulogix.kettle.lib;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public abstract class EasyStepDialog extends BaseStepDialog implements StepDialogInterface {
	
	protected String messagesPrefix = "EasyStep";
	
	/**
	 *	The PKG member is used when looking up internationalized strings.
	 *	The properties file with localized keys is expected to reside in 
	 *	{the package of the class specified}/messages/messages_{locale}.properties   
	 */
	protected Class<?> PKG; // for i18n purposes

	// this is the object the stores the step's settings
	// the dialog reads the settings from it when opening
	// the dialog writes the settings to it when confirmed 
	private EasyStepMeta meta;
		
	protected ModifyListener globalListener;
	
	protected Map<String, Control> controls;
	protected Map<String, String> controlTypes;
	
	
	/**
	 * The constructor should simply invoke super() and save the incoming meta
	 * object to a local variable, so it can conveniently read and write settings
	 * from/to it.
	 * 
	 * @param parent 	the SWT shell to open the dialog in
	 * @param in		the meta object holding the step's settings
	 * @param transMeta	transformation description
	 * @param sname		the step name
	 */
	public EasyStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		meta = (EasyStepMeta) in;
		
		// The ModifyListener used on all controls. It will update the meta object to 
		// indicate that changes are being made.
		globalListener = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				meta.setChanged();
			}
		};
		
		controls = new LinkedHashMap<String, Control>();
		controlTypes = new LinkedHashMap<String, String>();
	}
	
	protected void addControl(String name, Control control, String type) {
		controls.put(name, control);
		controlTypes.put(name, type);
	}
	
	protected Label addLabel(String stringToken, Composite composite, int middle, int margin, Control lastControl) {
	    Label label = new Label( composite, SWT.RIGHT );
	    label.setText( BaseMessages.getString( PKG, stringToken ) );
	    props.setLook( label );
	    FormData fdLabel = new FormData();
	    fdLabel.left = new FormAttachment( 0, 0 );
	    fdLabel.right = new FormAttachment( middle, -margin );
	    fdLabel.top = new FormAttachment( lastControl, margin );
	    label.setLayoutData( fdLabel );
	    
	    return label;
	}
	protected Text addTextField(String name, Control lastControl) {
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		return addTextField(name, lastControl, shell, middle, margin);
	}
	
	protected Text addTextField(String name, Control lastControl, Composite composite, int middle, int margin) {
		addLabel(messagesPrefix + ".Field." + name + ".Label", composite, middle, margin, lastControl);
		
		Text field = new Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(field);
		field.addModifyListener(globalListener);
		FormData fdField = new FormData();
		fdField.left = new FormAttachment(middle, 0);
		fdField.right = new FormAttachment(100, 0);
		fdField.top = new FormAttachment(lastControl, margin);
		field.setLayoutData(fdField);
		
		addControl(name, field, "TEXT");
		return field;
	}
	
	protected Button addCheckboxField(String name, Control lastControl) {
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		return addCheckboxField(name, lastControl, shell, middle, margin);
	}
	
	protected Button addCheckboxField(String name, Control lastControl, Composite composite, int middle, int margin) {
		addLabel(messagesPrefix + ".Field." + name + ".Label", composite, middle, margin, lastControl);
		
		Button field = new Button(composite, SWT.CHECK );
		props.setLook(field);
		FormData fdField = new FormData();
		fdField.left = new FormAttachment(middle, 0);
		fdField.right = new FormAttachment(100, 0);
		fdField.top = new FormAttachment(lastControl, margin);
		field.setLayoutData(fdField);
		
		addControl(name, field, "CHECKBOX");
		return field;
	}
	
	protected TextVar addTextVarField(String name, Control lastControl) {
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		return addTextVarField(name, lastControl, shell, middle, margin);
	}
	
	protected TextVar addTextVarField(String name, Control lastControl, Composite composite, int middle, int margin) {
		addLabel(messagesPrefix + ".Field." + name + ".Label", composite, middle, margin, lastControl);
		
		TextVar field = new TextVar(transMeta, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		field.setToolTipText( BaseMessages.getString( PKG, messagesPrefix + ".Field.textVars.Tooltip" ) );
		props.setLook(field);
		field.addModifyListener(globalListener);
		FormData fdField = new FormData();
		fdField.left = new FormAttachment(middle, 0);
		fdField.right = new FormAttachment(100, 0);
		fdField.top = new FormAttachment(lastControl, margin);
		field.setLayoutData(fdField);
		
		addControl(name, field, "TEXTVAR");
		return field;
	}
	
	protected CCombo addStreamFieldSelector(String name, Control lastControl) {
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		return addStreamFieldSelector(name, lastControl, shell, middle, margin);
	}
	
	protected CCombo addStreamFieldSelector(String name, Control lastControl, Composite composite, int middle, int margin) {
		addLabel(messagesPrefix + ".Field." + name + ".Label", composite, middle, margin, lastControl);
			 		
	    CCombo field = new CCombo( composite, SWT.BORDER | SWT.READ_ONLY );
	    props.setLook( field );
	    field.setEditable( true );
	    field.addModifyListener( globalListener );
	    FormData fdField = new FormData();
	    fdField.left = new FormAttachment( middle, 0 );
	    fdField.top = new FormAttachment( lastControl, margin );
	    fdField.right = new FormAttachment( 100, 0 );
	    field.setLayoutData( fdField );
	    field.addFocusListener( new FocusListener() {
	      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
	      }

	      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
	        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
	        shell.setCursor( busy );
	        populateWithPreviousStepFields((CCombo) e.getSource());
	        shell.setCursor( null );
	        busy.dispose();
	      }
	    } );

	    addControl(name, field, "CCOMBO");
	    return field;
	}
	
	protected CCombo addCCombo(String name, Control lastControl, ArrayList<String> values) {
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		return addCCombo(name, lastControl, shell, middle, margin, values);
	}
	
	protected CCombo addCCombo(String name, Control lastControl, Composite composite, int middle, int margin, ArrayList<String> values) {
		addLabel(messagesPrefix + ".Field." + name + ".Label", composite, middle, margin, lastControl);
		
		CCombo field = new CCombo( composite, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );

		for(int i=0; i<values.size(); i++) {
			field.add( values.get(i) );
		}
	    
	    field.select( 0 ); // +1: starts at -1
	    props.setLook( field );
	    FormData fdField = new FormData();
	    fdField.left = new FormAttachment( middle, 0 );
	    fdField.top = new FormAttachment( lastControl, margin );
	    fdField.right = new FormAttachment( 100, 0 );
	    field.setLayoutData( fdField );
	    
	    addControl(name, field, "CCOMBO");
	    return field;
	}
	
	protected void populateWithPreviousStepFields(CCombo combo) {
			try {
				String previousValue = combo.getText();

				combo.removeAll();
				RowMetaInterface r = transMeta.getPrevStepFields(stepname);
				if (r != null) {
					String[] fields = r.getFieldNames();
					combo.setItems(fields);

					if (previousValue != null) {
						combo.setText(previousValue);
					}
				}
			} catch (KettleException ke) {
				new ErrorDialog(
						shell,
						BaseMessages.getString(PKG,
								"EasyStep.FailedToGetFields.DialogTitle"),
						BaseMessages.getString(PKG,
								"EasyStep.FailedToGetFields.DialogMessage"),
						ke);
			}
	}
	
	/**
	 * This helper method puts the step configuration stored in the meta object
	 * and puts it into the dialog controls.
	 */
	protected void populateDialog() {
		wStepname.selectAll();
		String key = "", value = "";
		for (Map.Entry<String, Object> entry : meta.fields.entrySet()) {
			key 	= entry.getKey();
			value 	= entry.getValue().toString();
			switch(controlTypes.get(key)) {
				case "CCOMBO" 	: ((CCombo)controls.get(key)).setText(value); break;
				case "TEXTVAR" 	: ((TextVar)controls.get(key)).setText(value); break;
				case "TEXT" 	: ((Text)controls.get(key)).setText(value); break;	
				case "CHECKBOX" : ((Button)controls.get(key)).setSelection(value.equals("Y")); break;	
			}			
		}
	}
	
	/**
	 * populates the meta object with data coming from the UI
	 */
	protected void populateMeta() {
		// Setting the  settings to the meta object
		String key = "", value = "";
		for (Map.Entry<String, Object> entry : meta.fields.entrySet()) {
			key 	= entry.getKey();
			value 	= entry.getValue().toString();
			switch(controlTypes.get(key)) {
				case "CCOMBO" 	: meta.fields.put(key, ((CCombo)controls.get(key)).getText()); break;
				case "TEXTVAR" 	: meta.fields.put(key, ((TextVar)controls.get(key)).getText()); break;
				case "TEXT" 	: meta.fields.put(key, ((Text)controls.get(key)).getText()); break;
				case "CHECKBOX" : meta.fields.put(key, ((Button)controls.get(key)).getSelection() ? "Y" : "N" ); break;
			}			
		}
	}

	/**
	 * Called when the user cancels the dialog.  
	 */
	protected void cancel() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to null to indicate that dialog was cancelled.
		stepname = null;
		// Restoring original "changed" flag on the met aobject
		meta.setChanged(changed);
		// close the SWT dialog window
		dispose();
	}
	
	/**
	 * Called when the user confirms the dialog
	 */
	protected void ok() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to step name from the dialog control
		stepname = wStepname.getText(); 
		
		populateMeta();
		
		// close the SWT dialog window
		dispose();
	}

}
