package bdv.bigcat.viewer;


import javafx.scene.control.TextField;
import javafx.scene.control.CheckBox;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import bdv.bigcat.viewer.atlas.opendialog.BackendDialog;
import javafx.beans.property.SimpleObjectProperty;
import javafx.geometry.Insets;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;

public class IdSelectorDialog extends Dialog<String>
{
	private final SimpleObjectProperty< String > ids = new SimpleObjectProperty<>();
	
	private final SimpleObjectProperty< Boolean > append = new SimpleObjectProperty<>();


	public IdSelectorDialog()
	{
		System.out.println("constructor");
		this.setTitle( "Select ids" );
		this.getDialogPane().getButtonTypes().addAll( ButtonType.CANCEL, ButtonType.OK );
		
		final TextField idsField = new TextField();
		idsField.setPromptText("Enter ids separated by comma");
		idsField.setMinWidth( 0 );
		idsField.setMaxWidth( Double.POSITIVE_INFINITY );
		idsField.textProperty().bindBidirectional( ids );
		
		CheckBox box = new CheckBox("append to existing ids");
		box.setIndeterminate(false);
		box.setSelected(false);
		box.selectedProperty().bindBidirectional(append);
		append.set(false);
		
		final GridPane grid = new GridPane();
		grid.add( idsField, 0, 0 );
		grid.add( box, 0, 1 );
		GridPane.setHgrow( idsField, Priority.ALWAYS );
		GridPane.setHgrow( box, Priority.ALWAYS );

		this.getDialogPane().setContent( new VBox( 10, grid) );
	}
	
	public long[] getIds()
	{
		String ids = this.ids.get();
		List<Long> list = new ArrayList<Long>();
		for (String s : ids.split("\\,"))
		    list.add(Long.valueOf(s.trim()));
		
		long[] resultIds = list.stream().mapToLong(l -> l).toArray();

		return resultIds;
	}
	
	public boolean append()
	{
		return append.get();
	}
}
