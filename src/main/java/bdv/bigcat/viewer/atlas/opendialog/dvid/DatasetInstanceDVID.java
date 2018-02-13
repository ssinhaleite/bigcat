package bdv.bigcat.viewer.atlas.opendialog.dvid;

import java.util.List;

public class DatasetInstanceDVID
{

	static public class Base
	{
		public String Typename;

		public String TypeURL;

		public String TypeVersion;

		public String DataUUID;

		public String Name;

		public String RepoUUID;

		public String Compression;

		public String Checksum;

		public boolean Versioned;

	}

	static public class Extended
	{
		public List< Values > Values;

		public boolean Interpolable;

		public int[] BlockSize;

		public double[] VoxelSize;

		public String[] VoxelUnits;

		public double[] MinPoint;

		public double[] MaxPoint;

		public long[] MinIndex;

		public long[] MaxIndex;

		public int Background;
	}

	static public class Values
	{
		public String DataType;

		public String Label;
	}

	public Base Base;
	public Extended Extended;
}
