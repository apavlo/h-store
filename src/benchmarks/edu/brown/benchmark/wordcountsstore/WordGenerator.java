package edu.brown.benchmark.wordcountsstore;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;


public class WordGenerator {

    private int m_size;
    private List<String> m_words;
    
    private int m_position;
    
    /**
    public WordGenerator(int clientId, String strFileName) {
       
        m_size = 0;
        m_position = 0;
        m_words= new ArrayList<String>();
        
        // get the content of file
        FileInputStream fis;
        try {
            
            fis = new FileInputStream(strFileName);
            Scanner scanner = new Scanner(fis);
            
            while(scanner.hasNextLine()){
                String line_msg = scanner.nextLine();
                
                // current we use delimiter to split line into words 
                StringTokenizer st = new StringTokenizer(line_msg, " ,;.");
                while (st.hasMoreTokens()) {
                    m_words.add(st.nextToken());
                }
            }
            
            scanner.close();

            // get the size of the word array
            m_size = m_words.size();
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    } */
    
    public WordGenerator(int clientId, String strFileName) {
        
        m_size = 0;
        m_position = 0;
        m_words= new ArrayList<String>();
        
        m_words.add("Apple");
        m_words.add("Banana");
        m_words.add("Cantalope");
        m_words.add("Durian");
        
        m_size = m_words.size();
        
    } 
    
    public boolean isEmpty()
    {
        if( m_size == 0 )
            return true;
        else
            return false;
    }
    
    public void reset()
    {
        m_position = 0;
    }

    public boolean hasMoreWords()
    {
        if(m_position >= m_size)
            return false;
        else
            return true;
    }
    
    public String nextWord()
    {
        String word = m_words.get(m_position);
        m_position++;
        
        return word;
    }
    
}