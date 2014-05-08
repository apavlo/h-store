package edu.brown.stream;

import java.util.*;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class VoteGenerator {
    
    private ArrayList<PhoneCall> m_votes;
    
    private int m_position = 0;

    // orderedcall.ser or disorderedcall.ser
    public VoteGenerator(String strFileName) {
        m_position = 0;
        
        m_votes = PhoneCallAccessor.getPhoneCallsFromFile(strFileName);
    }
    
    public boolean isEmpty()
    {
        return m_votes.isEmpty();
    }
    
    public int size()
    {
        return this.m_votes.size();
    }

    public void reset()
    {
        m_position = 0;
    }
    
    public boolean hasMoreVotes()
    {
        int size = this.m_votes.size();
        //System.out.println("size and pos: " + size +"-"+m_position);
        if(m_position >= size)
            return false;
        else
            return true;
    }
    
    public synchronized PhoneCall nextVote()
    {
        if(hasMoreVotes()==false)
            return null;
        
        //System.out.println("get call at position: " + m_position);
        PhoneCall call = m_votes.get(m_position);
        m_position++;
        
        return call;
    }
}
