package main.spark.preprocessing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import main.spark.preprocessing.Document;
import main.spark.preprocessing.DocumentPreprocessor;
import main.spark.preprocessing.PreprocessorConstants;

public class LDAPreprocessor {

	private int rows;
	private int columns;
	private File ldaFile;	
	private DocumentPreprocessor documentPreprocessor;
	private List<Document> ldaDocuments;
	int [][] matrix;
	SparkConf sparkConf;
	JavaSparkContext javaSparkContext;
			
	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public int getColumns() {
		return columns;
	}

	public void setColumns(int columns) {
		this.columns = columns;
	}

	public LDAPreprocessor(String folder, SparkConf sparkConf, JavaSparkContext javaSparkContext) {
		this.sparkConf = sparkConf;
		this.javaSparkContext = javaSparkContext;
				
		ldaFile = new File(folder + File.pathSeparator + PreprocessorConstants.LDA_FILENAME); 
				
		documentPreprocessor = new DocumentPreprocessor();
		documentPreprocessor.loadStopWords();
		
		ldaDocuments = new ArrayList<Document>();
		
		File [] fileList = new File(folder).listFiles();
		setRows(fileList.length);
		
		for (final File file : fileList) {
			JavaRDD<Document> document = this.javaSparkContext.textFile(file.getAbsolutePath()).map(
				new Function<String, Document>() {
					@Override
					public Document call(String text) throws Exception {
						// TODO Auto-generated method stub
						Document doc = new Document();
						doc.setName(file.getName());
						doc.setContent(documentPreprocessor.changeCase(text));			
						documentPreprocessor.refineDocument(doc);
						documentPreprocessor.countFrequency(doc);
						ldaDocuments.add(doc);
						return doc;
					}
				}
			);  
		}
		
		setColumns(DocumentPreprocessor.dictionary.size());
		
		createMatrix();
		printLDAdata();
	}
	
	public void createMatrix() {
		matrix = new int[getRows()][getColumns()];
		
		int rowCount = 0;
		for (Iterator iterator = ldaDocuments.iterator(); iterator.hasNext();) {
			Document document = (Document) iterator.next();
			matrix[rowCount] = returnColumn(document);
			rowCount ++;
		}
		
	}
	
	public int[] returnColumn(Document doc) {
		int [] col = new int[getRows()];
		
		int colCount = 0;
		
		for (Iterator iterator = DocumentPreprocessor.dictionary.iterator(); iterator.hasNext();) {
			String dictWord = (String) iterator.next();
			
			if(doc.getUniqueWords().contains(dictWord) && doc.getWordFrequency().containsKey(dictWord)) {
				col[colCount] = doc.getWordFrequency().get(dictWord);
			} else {
				col[colCount] = 0;
			}
			
			colCount ++;
		}
		
		return col;
	}
	
	public void printLDAdata() {
		StringBuffer stringBuffer = new StringBuffer();
		
		for (int i = 0; i < matrix.length; i++) {
			String row = "";
			for (int j = 0; j < matrix[i].length; j++) {
				row = row + matrix[i][j] + " ";
			}
			stringBuffer.append(row + "\n");
		}
		
		try {
			PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(ldaFile)));
			printWriter.println(stringBuffer);
			printWriter.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args == null || args.length < 1) {
			System.out.println("Folder parameter is not present");
			System.exit(0);
		} 

		if(!(new File(args[0])).exists() || !(new File(args[0])).isDirectory()) {
			System.out.println("Folder doesn't exists or not a folder");
			System.exit(0);
		}
		
		if((new File(args[0])).listFiles().length < 1) {
			System.out.println("Folder is empty");
			System.exit(0);
		}
				
		SparkConf sparkConf = new SparkConf().setAppName("LDAPreprocessor");
	    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		new LDAPreprocessor(args[0], sparkConf, javaSparkContext);
	}

}
