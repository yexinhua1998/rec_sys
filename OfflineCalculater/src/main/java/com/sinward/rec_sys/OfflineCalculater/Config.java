package com.sinward.rec_sys.OfflineCalculater;

public class Config {
	public static String WORD_MATRIX_PATH="/home/sinward/rec_sys/OfflineCalculater/wordvec.ejml.SimpleMatrix";
	public static String WORD2ID_PATH="/home/sinward/rec_sys/OfflineCalculater/word2id.HashMap";
	public static int WORD_VEC_DIM=300;
	public static int DOC_NUM=1656941;//1656941
	public static class DataSource{
		public static int NUM_OF_PARTITIONS=16600;//16600
		public static int LOWER_BOUND=1;
		public static int UPPER_BOUND=1660000;//1660000
	}
	public static class File{
		public static String RAW_VEC_PATH="/home/sinward/rec_sys/OfflineCalculater/sgns.target.word-word.dynwin5.thr10.neg5.dim300.iter5";
		public static String WORD2ID_PATH="/home/sinward/rec_sys/OfflineCalculater/word2id.HashMap";
		public static String WORD_MATRIX_PATH="/home/sinward/rec_sys/OfflineCalculater/wordvec.Spark.Matrix";
	}
	public static class DB{
		public static String JDBC_URL="jdbc:postgresql://192.168.59.134:5432/sina_page";
		public static String USER="postgres";
		public static String PASSWORD="Ye25554160";
	}
}
