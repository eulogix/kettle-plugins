package com.eulogix.kettle.steps.pst_input;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;

import com.eulogix.kettle.lib.EasyStepMeta;

@Step(	
		id = "EulogixPstInputStep",
		image = "pst-input.png",
		i18nPackageName="com.eulogix.kettle.steps.pst_input",
		name="EulogixPstInputStep.Name",
		description = "EulogixPstInputStep.TooltipDesc",
		categoryDescription="EulogixPstInputStep.Category"
)
public class PstInputStepMeta extends EasyStepMeta {

	/**
	 *	The PKG member is used when looking up internationalized strings.
	 *	The properties file with localized keys is expected to reside in 
	 *	{the package of the class specified}/messages/messages_{locale}.properties   
	 */
	private static Class<?> PKG = PstInputStepMeta.class; // for i18n purposes
	
	/**
	 * Constructor should call super() to make sure the base class has a chance to initialize properly.
	 */
	public PstInputStepMeta() {
		super();
	}
	
	public void setUpFields() {
		fields = new LinkedHashMap<String, Object>();
		fields.put("fileName", "");
	}
	
	/**
	 * Called by Spoon to get a new instance of the SWT dialog for the step.
	 * A standard implementation passing the arguments to the constructor of the step dialog is recommended.
	 * 
	 * @param shell		an SWT Shell
	 * @param meta 		description of the step 
	 * @param transMeta	description of the the transformation 
	 * @param name		the name of the step
	 * @return 			new instance of a dialog for this step 
	 */
	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new PstInputStepDialog(shell, meta, transMeta, name);
	}

	/**
	 * Called by PDI to get a new instance of the step implementation. 
	 * A standard implementation passing the arguments to the constructor of the step class is recommended.
	 * 
	 * @param stepMeta				description of the step
	 * @param stepDataInterface		instance of a step data class
	 * @param cnr					copy number
	 * @param transMeta				description of the transformation
	 * @param disp					runtime implementation of the transformation
	 * @return						the new instance of a step implementation 
	 */
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
		return new PstInputStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	/**
	 * Called by PDI to get a new instance of the step data class.
	 */
	public StepDataInterface getStepData() {
		return new PstInputStepData();
	}	

	/**
	 * This method is called every time a new step is created and should allocate/set the step configuration
	 * to sensible defaults. The values set here will be used by Spoon when a new step is created.    
	 */
	public void setDefault() {
	}

	/**
	 * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
	 * step meta object. Be sure to create proper deep copies if the step configuration is stored in
	 * modifiable objects.
	 * 
	 * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
	 * a deep copy.
	 * 
	 * @return a deep copy of this
	 */
	public Object clone() {
		PstInputStepMeta retval = (PstInputStepMeta) super.clone();
		retval.setUpFields();
		return retval;
	}
	
	/**
	 * This method is called to determine the changes the step is making to the row-stream.
	 * To that end a RowMetaInterface object is passed in, containing the row-stream structure as it is when entering
	 * the step. This method must apply any changes the step makes to the row stream. Usually a step adds fields to the
	 * row-stream.
	 * 
	 * @param inputRowMeta		the row structure coming in to the step
	 * @param name 				the name of the step making the changes
	 * @param info				row structures of any info steps coming in
	 * @param nextStep			the description of a step this step is passing rows to
	 * @param space				the variable space for resolving variables
	 * @param repository		the repository instance optionally read from
	 * @param metaStore			the metaStore to optionally read from
	 */
	public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException{

		ArrayList<ValueMetaInterface> metas = new ArrayList<ValueMetaInterface>(); 

		// a value meta object contains the meta data for a field
		metas.add( new ValueMeta("descriptor_node_id", ValueMeta.TYPE_INTEGER) );
		metas.add( new ValueMeta("folder", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("date", ValueMeta.TYPE_DATE) );
		metas.add( new ValueMeta("subject", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("body", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("body_html", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("body_prefix", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("sender_name", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("sender_email_address", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("to", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("cc", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("bcc", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("size", ValueMeta.TYPE_INTEGER) );
		metas.add( new ValueMeta("number_of_attachments", ValueMeta.TYPE_INTEGER) );
		metas.add( new ValueMeta("number_of_recipients", ValueMeta.TYPE_INTEGER) );
		metas.add( new ValueMeta("importance", ValueMeta.TYPE_INTEGER) );
		metas.add( new ValueMeta("in_reply_to_id", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("message_class", ValueMeta.TYPE_STRING) );
		metas.add( new ValueMeta("message_cc_me", ValueMeta.TYPE_BOOLEAN) );
		metas.add( new ValueMeta("message_to_me", ValueMeta.TYPE_BOOLEAN) );
		metas.add( new ValueMeta("reply_requested", ValueMeta.TYPE_BOOLEAN) );
		metas.add( new ValueMeta("forwarded", ValueMeta.TYPE_BOOLEAN) );
		metas.add( new ValueMeta("read", ValueMeta.TYPE_BOOLEAN) );
		metas.add( new ValueMeta("replied", ValueMeta.TYPE_BOOLEAN) );
		metas.add( new ValueMeta("from_me", ValueMeta.TYPE_BOOLEAN) );
		
		for(int i=0;i<metas.size();i++) {
			metas.get(i).setTrimType(ValueMeta.TRIM_TYPE_BOTH);
			metas.get(i).setOrigin(name);
			inputRowMeta.addValueMeta(metas.get(i));
		}
			
	}

}
