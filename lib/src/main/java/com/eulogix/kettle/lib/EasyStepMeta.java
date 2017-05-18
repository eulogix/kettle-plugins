package com.eulogix.kettle.lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public abstract class EasyStepMeta extends BaseStepMeta implements StepMetaInterface {
	
	/**
	 *	The PKG member is used when looking up internationalized strings.
	 *	The properties file with localized keys is expected to reside in 
	 *	{the package of the class specified}/messages/messages_{locale}.properties   
	 */
	private static Class<?> PKG = EasyStepMeta.class; // for i18n purposes
	
	public LinkedHashMap<String, Object> fields;
	
	/**
	 * Constructor should call super() to make sure the base class has a chance to initialize properly.
	 */
	public EasyStepMeta() {
		super();
		this.setUpFields();
	}
	
	public void setUpFields() {
	}
	
	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
	 * return value is an XML fragment consisting of one or more XML tags.  
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
	 * 
	 * @return a string containing the XML serialization of this step
	 */
	public String getXML() throws KettleValueException {		
		String xml="";
		for (Map.Entry<String, Object> entry : fields.entrySet()) {
		    xml += XMLHandler.addTagValue(entry.getKey(), entry.getValue().toString());
		}
		return xml;
	}

	/**
	 * builds a table data XML section
	 * @param tagName
	 * @param data
	 * @return
	 */
	protected String getTableXML(String tagName, ArrayList<HashMap<String,String>> data) {
		StringBuilder retval = new StringBuilder(300);
		String xmlLine;
		
		retval.append( "<" + tagName + ">" ).append( Const.CR );
	    
		for ( HashMap<String, String> line : data ) {
			retval.append("<line>" );
			
			for (Map.Entry<String, String> entry : line.entrySet()) {
				xmlLine = XMLHandler.addTagValue(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
			    retval.append(xmlLine);
			}
	      
	      retval.append( "</line>" ).append( Const.CR );
	    }
	    retval.append( "</" + tagName + ">" ).append( Const.CR );
	    
	    return retval.toString();
	}
	
	/**
	 * This method is called by PDI when a step needs to load its configuration from XML.
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently read from the
	 * XML node passed in.
	 * 
	 * @param stepnode	the XML node containing the configuration
	 * @param databases	the databases available in the transformation
	 * @param metaStore the metaStore to optionally read from
	 */
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {

		try {
			for (Map.Entry<String, Object> entry : fields.entrySet()) {
				String XMLValue = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, entry.getKey()));
				fields.put(entry.getKey(), XMLValue == null ? "" : XMLValue);
			}
		} catch (Exception e) {
			throw new KettleXMLException("Plugin unable to read step info from XML node", e);
		}

	}	
	
	/**
	 * returns a data structure from a xml node, used for grids
	 * @param stepnode
	 * @param tagName
	 * @param databases
	 * @param metaStore
	 * @return
	 * @throws KettleXMLException
	 */
	protected ArrayList<HashMap<String, String>> loadXMLTableData(Node stepnode, String tagName, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		ArrayList<HashMap<String, String>> ret = new ArrayList<HashMap<String, String>>();
		
		Node datanode = XMLHandler.getSubNode(stepnode, tagName);
		Node lineNode = datanode.getFirstChild();
		
		while (lineNode != null) {
			if ("line".equals(lineNode.getNodeName())) {
				HashMap<String, String> line = new HashMap<String, String>();
				Node itemNode = lineNode.getFirstChild();
				while (itemNode != null) {
					if(itemNode.getNodeType() == Node.ELEMENT_NODE)
						line.put(itemNode.getNodeName(), XMLHandler.getNodeValue(itemNode));
					itemNode = itemNode.getNextSibling();
				}
				ret.add(line);
			}
			lineNode = lineNode.getNextSibling();
		}
		return ret;
	}
	
	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to a repository.
	 * The repository implementation provides the necessary methods to save the step attributes.
	 *
	 * @param rep					the repository to save to
	 * @param metaStore				the metaStore to optionally write to
	 * @param id_transformation		the id to use for the transformation when saving
	 * @param id_step				the id to use for the step  when saving
	 */
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException
	{
		try{
			for (Map.Entry<String, Object> entry : fields.entrySet()) {
				rep.saveStepAttribute(id_transformation, id_step, entry.getKey(), entry.getValue().toString());
			}
		}
		catch(Exception e){
			throw new KettleException("Unable to save step into repository: "+id_step, e); 
		}
	}		
	
	/**
	 * saves a table data structure to the repo. untested!
	 * @param data
	 * @param dataUid
	 * @param rep
	 * @param metaStore
	 * @param idTransformation
	 * @param idStep
	 * @throws KettleException
	 */
	public void saveTableToRep(ArrayList<HashMap<String, String>> data, String dataUid, Repository rep, IMetaStore metaStore, ObjectId idTransformation, ObjectId idStep) throws KettleException 
	{
		try {
			Gson gson = new GsonBuilder().create();
			rep.saveStepAttribute( idTransformation, idStep, dataUid + "_nr_rows", data.size() );
			for ( int i = 0; i < data.size(); i++ ) {
				  HashMap<String, String> line = data.get( i );
				  rep.saveStepAttribute( idTransformation, idStep, i, dataUid + "_row", gson.toJson(line) );	
		    }
		} catch(Exception e){
			throw new KettleException("Unable to save step into repository: "+idStep, e); 
		}
	}
	
	/**
	 * This method is called by PDI when a step needs to read its configuration from a repository.
	 * The repository implementation provides the necessary methods to read the step attributes.
	 * 
	 * @param rep		the repository to read from
	 * @param metaStore	the metaStore to optionally read from
	 * @param id_step	the id of the step being read
	 * @param databases	the databases available in the transformation
	 * @param counters	the counters available in the transformation
	 */
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException  {
		try{
			for (Map.Entry<String, Object> entry : fields.entrySet()) {
				String RepValue = rep.getStepAttributeString(id_step, entry.getKey());
				fields.put(entry.getKey(), RepValue == null ? "" : RepValue);
			}
		}
		catch(Exception e){
			throw new KettleException("Unable to load step from repository", e);
		}
	}
	
	/**
	 * reads a table from the repository. untested
	 * @param dataUid
	 * @param rep
	 * @param metaStore
	 * @param idStep
	 * @param databases
	 * @return
	 * @throws KettleException
	 */
	public ArrayList<HashMap<String, String>> readTableFromRep(String dataUid, Repository rep, IMetaStore metaStore, ObjectId idStep, List<DatabaseMeta> databases) throws KettleException  
	{
		ArrayList<HashMap<String, String>> ret = new ArrayList<HashMap<String, String>>();		
		int nrLines = (int) rep.getStepAttributeInteger( idStep, dataUid + "_nr_rows" );
		java.lang.reflect.Type stringStringMap = new TypeToken<HashMap<String, String>>(){}.getType();
	    for ( int i = 0; i < nrLines; i++ ) {	
	    	String json = rep.getStepAttributeString( idStep, i, dataUid + "_row" );
	    	JsonElement root = new JsonParser().parse(json);
	    	HashMap<String, String> line = new Gson().fromJson(root, stringStringMap);
	        ret.add( line );
	    }
	    return ret;
	}
	      
	/**
	 * aids in deep cloning
	 * @param data
	 * @return
	 */
	public ArrayList<HashMap<String,String>> deepCloneTableData(ArrayList<HashMap<String,String>> data) {
		ArrayList<HashMap<String,String>> clone = new ArrayList<HashMap<String,String>>();
		
		for (HashMap<String, String> line : data) {
			HashMap<String, String> newLine = new HashMap<String, String>();
			for (Map.Entry<String, String> entry : line.entrySet()) {
				newLine.put( entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
			}
			clone.add(newLine);
		  }
		
		return clone;
	}
	
	/**
	 * This method is called when the user selects the "Verify Transformation" option in Spoon. 
	 * A list of remarks is passed in that this method should add to. Each remark is a comment, warning, error, or ok.
	 * The method should perform as many checks as necessary to catch design-time errors.
	 * 
	 * Typical checks include:
	 * - verify that all mandatory configuration is given
	 * - verify that the step receives any input, unless it's a row generating step
	 * - verify that the step does not receive any input if it does not take them into account
	 * - verify that the step finds fields it relies on in the row-stream
	 * 
	 *   @param remarks		the list of remarks to append to
	 *   @param transmeta	the description of the transformation
	 *   @param stepMeta	the description of the step
	 *   @param prev		the structure of the incoming row-stream
	 *   @param input		names of steps sending input to the step
	 *   @param output		names of steps this step is sending output to
	 *   @param info		fields coming in from info steps 
	 *   @param metaStore	metaStore to optionally read from
	 */
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore)  {
		
		CheckResult cr;

		// See if there are input streams leading to this step!
		if (input.length > 0) {
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "EasyStep.CheckResult.ReceivingRows.OK"), stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "EasyStep.CheckResult.ReceivingRows.ERROR"), stepMeta);
			remarks.add(cr);
		}	
    	
	}
}
