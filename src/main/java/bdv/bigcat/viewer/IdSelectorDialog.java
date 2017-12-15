package bdv.bigcat.viewer;

import java.util.ArrayList;
import java.util.List;

import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Control;
import javafx.scene.control.Dialog;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

public class IdSelectorDialog extends Dialog< ButtonType >
{
	private final SimpleObjectProperty< String > ids = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Boolean > append = new SimpleObjectProperty<>();

	private final SimpleBooleanProperty isEmpty = new SimpleBooleanProperty();

	public IdSelectorDialog()
	{
		this.setTitle( "Select id for 2D highlight" );
		this.getDialogPane().getButtonTypes().addAll( ButtonType.CANCEL, ButtonType.OK );
		this.getDialogPane().setMinSize( 260, 100 );
		this.setResizable( true );

		final TextField idsField = new TextField();
		idsField.setPromptText( "Enter ids separated by comma" );
		idsField.setMinWidth( Control.USE_PREF_SIZE );
		idsField.setMaxWidth( Double.POSITIVE_INFINITY );
		idsField.textProperty().bindBidirectional( ids );

		CheckBox box = new CheckBox( "append to existing ids" );
		box.setIndeterminate( false );
		box.setSelected( false );
		box.selectedProperty().bindBidirectional( append );
		append.set( false );

		this.ids.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() )
				this.isEmpty.set( false );
			else
				this.isEmpty.set( true );
		} );

		this.isEmpty.set( true );
		this.getDialogPane().lookupButton( ButtonType.OK ).disableProperty().bind( this.isEmpty );

		final GridPane grid = new GridPane();
		grid.add( idsField, 0, 0 );
		grid.add( box, 0, 1 );
		grid.setVgap( 5 );
		GridPane.setHgrow( idsField, Priority.ALWAYS );
		GridPane.setHgrow( box, Priority.ALWAYS );

		this.getDialogPane().setContent( new VBox( 10, grid ) );
	}

	public long[] getIds()
	{
		String ids = this.ids.get();
		List< Long > list = new ArrayList< Long >();
		for ( String s : ids.split( "\\," ) )
			list.add( Long.valueOf( s.trim() ) );

		long[] resultIds = list.stream().mapToLong( l -> l ).toArray();

		return resultIds;
	}

	public boolean append()
	{
		return append.get();
	}
}
