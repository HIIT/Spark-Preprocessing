package main.spark.preprocessing;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Document {

	private String name;
	private int label;
	private String content;
	private String refinedContent;
	private List<String> words;
	private Set<String> uniqueWords;
	private Map<String, Integer> wordFrequency; 
	private String libsvmData;
		
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getLabel() {
		return label;
	}

	public void setLabel(int label) {
		this.label = label;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getRefinedContent() {
		return refinedContent;
	}

	public void setRefinedContent(String refinedContent) {
		this.refinedContent = refinedContent;
	}

	public List<String> getWords() {
		return words;
	}

	public void setWords(List<String> words) {
		this.words = words;
	}

	public Set<String> getUniqueWords() {
		return uniqueWords;
	}

	public void setUniqueWords(Set<String> uniqueWords) {
		this.uniqueWords = uniqueWords;
	}

	public Map<String, Integer> getWordFrequency() {
		return wordFrequency;
	}

	public void setWordFrequency(Map<String, Integer> wordFrequency) {
		this.wordFrequency = wordFrequency;
	}

	public String getLibsvmData() {
		return libsvmData;
	}

	public void setLibsvmData(String libsvmData) {
		this.libsvmData = libsvmData;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
