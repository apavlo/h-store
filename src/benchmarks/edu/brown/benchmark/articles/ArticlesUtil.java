package edu.brown.benchmark.articles;

import java.util.Random;

public abstract class ArticlesUtil {

    public static final Random rand = new Random();

    /**
     * Compute a unique CommentId for the articleId and the current
     * number of comments. 
     * @param articleId
     * @param num_comments
     * @return
     */
    public static long getCommentId(long articleId, long num_comments) {
        // The upper 32-bits are the articleId
        // The lower 16-bits are the num_comments
        return (num_comments | articleId<<32); 
    }
    
    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = (int)(minimum_length == maximum_length ?
                                minimum_length :
                                number(minimum_length, maximum_length));
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextInt()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }
}
