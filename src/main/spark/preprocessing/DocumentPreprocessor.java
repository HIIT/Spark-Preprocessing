package main.spark.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.booleanValue_return;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;
import org.apache.mina.filter.buffer.BufferedWriteFilter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class DocumentPreprocessor {

	private DocumentPreprocessor documentPreprocessor;
	public Set<String> stopWords;
	public static Set<String> dictionary = new HashSet<String>();
	SparkConf sparkConf;
	JavaSparkContext javaSparkContext;
	
	public DocumentPreprocessor() {
		// TODO Auto-generated constructor stub
	}
	
	public DocumentPreprocessor(final String sourceFolder, final String destinationFolder, SparkConf sparkConf, JavaSparkContext javaSparkContext) {
		// TODO Auto-generated constructor stub
		this.sparkConf = sparkConf;
		this.javaSparkContext = javaSparkContext;
		
		documentPreprocessor = new DocumentPreprocessor();
		documentPreprocessor.loadStopWords();
		
		File [] fileList = new File(sourceFolder).listFiles();
				
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
						doc.setLibsvmData(documentPreprocessor.arrangeData(doc));
						documentPreprocessor.writeLibsvmData(destinationFolder, doc);				
						return doc;
					}
				}
			);  
		}
	}
	
	public void loadStopWords() {
		try {
			stopWords = new HashSet<String>();
			@SuppressWarnings("resource")
			BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(PreprocessorConstants.STOP_WORDS)));
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				line = line.trim();
				stopWords.add(line);
			}
			bufferedReader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String readDocument(String fileName) {
		StringBuilder content = new StringBuilder();
		try {

			@SuppressWarnings("resource")
			BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(fileName)));
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				content.append(line);
			}
			bufferedReader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return content.toString();
	}
	
	public void writeDocument(String data, boolean isTrain) {
		try {
			String fileName = isTrain ? PreprocessorConstants.TRAIN_LIBSVM : PreprocessorConstants.TEST_LIBSVM;
			PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(fileName), true)));
			printWriter.println(data);
			printWriter.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeLibsvmData(String folder, Document doc) {
		try {
			String fileName = folder + File.pathSeparator + doc.getName() + ".libsvm";
			PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(fileName), true)));
			printWriter.println(doc.getLibsvmData());
			printWriter.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void refineDocument(Document doc) {
		TokenStream tokenStream = new StandardTokenizer(Version.LUCENE_36, new StringReader(doc.getContent()));
        tokenStream = new StopFilter(Version.LUCENE_36, tokenStream, stopWords);
        tokenStream = new PorterStemFilter(tokenStream);
 
        StringBuilder sb = new StringBuilder();
        CharTermAttribute charTermAttr = tokenStream.getAttribute(CharTermAttribute.class);
        
        List<String> words = new ArrayList<String>();
    	Set<String> uniqueWords = new HashSet<String>();
        
        try{
            while (tokenStream.incrementToken()) {
            	
            	String word = charTermAttr.toString();
//            	int wordVal = textToInt(charTermAttr.toString());
            	words.add(word);
            	uniqueWords.add(word);
            	dictionary.add(word);
            	
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(charTermAttr.toString());
            }
        }
        catch (IOException e){
            System.out.println(e.getMessage());
        }
        
        doc.setRefinedContent(sb.toString());
        doc.setWords(words);
        doc.setUniqueWords(uniqueWords);
	}
	
	public void countFrequency(Document doc) {
		Map<String, Integer> wordFrequency = new HashMap<String, Integer>();
		
		for (Iterator iterator = doc.getUniqueWords().iterator(); iterator.hasNext();) {
			String word = (String) iterator.next();
			int count = Collections.frequency(doc.getWords(), word);
			wordFrequency.put(word, count);
		}
		
		doc.setWordFrequency(wordFrequency);
	}
	
	public int textToInt(String text) {
		byte[] b = text.getBytes(StandardCharsets.US_ASCII);
		return new BigInteger(b).intValue();
	}
	
	public String changeCase(String text) {
		return text.toLowerCase();
	}
	
	public String arrangeData(Document doc) {
		List<String> dict = new ArrayList<String>(dictionary);
		
		String data = doc.getLabel() + " ";
		
		Iterator entries = doc.getWordFrequency().entrySet().iterator();
		while (entries.hasNext()) {
		  Entry thisEntry = (Entry) entries.next();
		  int index = dict.indexOf(thisEntry.getKey());
		  data = data + (index + 1) + ":" + thisEntry.getValue() + " ";
		}
		
		return data;
	}
		
	public static void main(String[] args) {			
		// TODO Auto-generated method stub
		if(args == null || args.length !=2) {
			System.out.println("Argument missing. It should contain source folder and destination folder");
			System.exit(0);
		} 

		if(!(new File(args[0])).exists() || !(new File(args[0])).isDirectory()) {
			System.out.println("Source folder doesn't exists or not a folder");
			System.exit(0);
		}
		
		if((new File(args[0])).listFiles().length < 1) {
			System.out.println("Source folder is empty");
			System.exit(0);
		}
		
		if((args[1] == null) && args[1].isEmpty()) {
			System.out.println("Destination folder is not provided");
			System.exit(0);
		}
		
		if(!(new File(args[1])).exists() || !(new File(args[1])).isDirectory()) {
			new File(args[1]).mkdir();
		}
				
		SparkConf sparkConf = new SparkConf().setAppName("DocumentPreprocessor");
	    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
	    
		new DocumentPreprocessor(args[0], args[1], sparkConf, javaSparkContext);
	}

}
