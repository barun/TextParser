package com.cts.hkp.indexer;

import java.util.Date;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Date;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
public class Indexer {
	private static String FILE_LOC = "D:\\barun\\src\\";
	
	public static void main(String[] args) {
		boolean create = true;
		String indexPath = "index";
		String docsPath = "D:\\barun\\src\\";
		String smsid = "qwewe1";
		Date creationDate = new Date();
		File docDir = new File(docsPath);
		Date start = new Date();
	    try
	    {
	      System.out.println("Indexing to directory '" + indexPath + "'...");
	      
	      Directory dir = FSDirectory.open(new File(indexPath));
	      
	      Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
	      IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
	   //   if (create) {
	        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
	  //    } else {
	  //      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
	 //     }
	      IndexWriter writer = new IndexWriter(dir, iwc);
	      indexDocs(writer, docDir,smsid,creationDate);
	      writer.close();
	      Date end = new Date();
	      System.out.println(end.getTime() - start.getTime() + " total milliseconds");
	     
	// --------------------------------------------------------------- //
	      //query perser
	      String field = "contents";
	      IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
	      IndexSearcher searcher = new IndexSearcher(reader);
	      QueryParser parser = new QueryParser(Version.LUCENE_47, field, analyzer);
	     // Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
	      String line = "Congratulaion AND baby";
	      
	      Query query = parser.parse(line);
	      
	      TopDocs results = searcher.search(query, null, 100);
	      ScoreDoc[] hits = results.scoreDocs;
	      System.out.println("hits : "+hits.length);
	    
	    }
	    catch (IOException e)
	    {
	      System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
	    } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
		
	
	
	
	
	static void indexDocs(IndexWriter writer, File file,String smsId, Date timestamp)
    throws IOException
  {
	
    if (file.canRead()) {
      if (file.isDirectory())
      {
        String[] files = file.list();
        if (files != null) {
          for (int i = 0; i < files.length; i++) {
            indexDocs(writer, new File(file, files[i]),smsId,timestamp);
          }
        }
      }
      else
      {
        FileInputStream fis;
        try
        {
          fis = new FileInputStream(file);
        }
        catch (FileNotFoundException fnfe)
        {
          return;
        }
        try
        {
          Document doc = new Document();
          Field pathField = new StringField("path", file.getPath(), Field.Store.YES);
          doc.add(pathField);
          doc.add(new TextField("smsid", smsId, Field.Store.YES));
          doc.add(new TextField("timestamp", timestamp.toString(), Field.Store.YES));
          doc.add(new LongField("modified", file.lastModified(), Field.Store.YES));
          doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(fis, "UTF-8"))));
          if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE)
          {
            System.out.println("adding " + file);
            writer.addDocument(doc);
          }
          else
          {
            System.out.println("updating " + file);
            writer.updateDocument(new Term("path", file.getPath()), doc);
          }
        }
        finally
        {
          fis.close();
        }
      }
    }
  }

}
