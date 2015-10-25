package main.spark.preprocessing;

import java.io.File;
import java.util.Iterator;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;

public class MainClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DocumentPreprocessor documentPreprocessor = new DocumentPreprocessor();
		documentPreprocessor.loadStopWords();
		
		File trainPosFolder = new File(PreprocessorConstants.TRAIN_POSITIVE_DOCUMENT);
		File [] trainPosFiles = trainPosFolder.listFiles();
		
		for (File file : trainPosFiles) {
			Document doc = new Document();
			doc.setName(file.getName());
			doc.setLabel(1);
			doc.setContent(documentPreprocessor.changeCase(documentPreprocessor.readDocument(file.getAbsolutePath())));
			
			documentPreprocessor.refineDocument(doc);
			documentPreprocessor.countFrequency(doc);
			documentPreprocessor.writeDocument(documentPreprocessor.arrangeData(doc), true);
		}
		
		File trainNegFolder = new File(PreprocessorConstants.TRAIN_NEGATIVE_DOCUMENT);
		File [] trainNegFiles = trainNegFolder.listFiles();
		
		for (File file : trainNegFiles) {
			Document doc = new Document();
			doc.setName(file.getName());
			doc.setLabel(0);
			doc.setContent(documentPreprocessor.changeCase(documentPreprocessor.readDocument(file.getAbsolutePath())));
			
			documentPreprocessor.refineDocument(doc);
			documentPreprocessor.countFrequency(doc);
			documentPreprocessor.writeDocument(documentPreprocessor.arrangeData(doc), true);
		}
		
		File testPosFolder = new File(PreprocessorConstants.TEST_POSITIVE_DOCUMENT);
		File [] testPosFiles = testPosFolder.listFiles();
		
		for (File file : testPosFiles) {
			Document doc = new Document();
			doc.setName(file.getName());
			doc.setLabel(1);
			doc.setContent(documentPreprocessor.changeCase(documentPreprocessor.readDocument(file.getAbsolutePath())));
			
			documentPreprocessor.refineDocument(doc);
			documentPreprocessor.countFrequency(doc);
			documentPreprocessor.writeDocument(documentPreprocessor.arrangeData(doc), false);
		}
		
		File testNegFolder = new File(PreprocessorConstants.TEST_NEGATIVE_DOCUMENT);
		File [] testNegFiles = testNegFolder.listFiles();
		
		for (File file : testNegFiles) {
			Document doc = new Document();
			doc.setName(file.getName());
			doc.setLabel(0);
			doc.setContent(documentPreprocessor.changeCase(documentPreprocessor.readDocument(file.getAbsolutePath())));
			
			documentPreprocessor.refineDocument(doc);
			documentPreprocessor.countFrequency(doc);
			documentPreprocessor.writeDocument(documentPreprocessor.arrangeData(doc), false);
		}
	}

}
