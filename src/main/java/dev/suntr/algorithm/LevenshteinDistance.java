package dev.suntr.algorithm;

import net.sourceforge.pinyin4j.PinyinHelper;

import java.util.Arrays;

public class LevenshteinDistance {

    private static char[] NUMBER_LATIN = new char[]{0x0020, 0x007F};
    private static char[] BASIC_CHINESE = new char[]{0x4E00, 0x9FA5};

    private static final int STEP = 100;
    private static final int MIN_LENGTH = 1;

    public static int distance(CharSequence source, CharSequence target, boolean excludeSpecial) {
        int srcLen = source.length();
        int tarLen = target.length();
        if (srcLen==0) return tarLen;
        if (tarLen == 0) return srcLen;
        if ((srcLen <= MIN_LENGTH || tarLen<=MIN_LENGTH) && !source.equals(target) ){
            return srcLen;
        }
        int[][] distance = new int[srcLen+1][tarLen+1];
        for (int i = 0; i <= srcLen; i++){
            distance[i][0] = i;
        }
        for (int i = 0; i <= tarLen; i++){
            distance[0][i] = i;
        }
        int equal;
        for (int i = 0; i < srcLen; i++){
            for (int j = 0; j <tarLen; j++) {
                char src = source.charAt(i);
                char tag = target.charAt(j);
                if (src == tag) {
                    equal = 0;
                } else {
                    if (!excludeSpecial) {
                        equal = 1;
                    } else {
                        if (isSpecialChar(src)) {
                            // source 为特殊字符 距离保持为source前一字符的距离
                            distance[i+1][j+1] = distance[i][j+1];
                            continue;
                        } else if (isSpecialChar(tag)) {
                            // target为特殊字符  距离保持为target前一字符的距离
                            distance[i+1][j+1] = distance[i+1][j];
                            continue;
                        } else {
                            equal = 1;
                        }
                    }
                }
                distance[i+1][j+1] = min(distance[i][j]+equal, distance[i][j+1]+1, distance[i+1][j]+1);
            }
        }
        return distance[srcLen][tarLen];
    }

    public static double similarity(CharSequence source, CharSequence target,
                               boolean excludeSpecial,
                               boolean caseInsensitive,
                               boolean samePronounce) {
        return similarity(new int[source.length()+1][target.length()+1], source, target, excludeSpecial, caseInsensitive, samePronounce);
    }

    public static double similarity(int[][] matrix,
                                    CharSequence source,
                                    CharSequence target,
                                    boolean excludeSpecial,
                                    boolean caseInsensitive,
                                    boolean samePronounce) {
        int srcLen = source.length();
        int tarLen = target.length();
        if (srcLen==0) return tarLen;
        if (tarLen == 0) return srcLen;
        if ((srcLen <= MIN_LENGTH || tarLen<=MIN_LENGTH) && !source.equals(target) ){
            return 0;
        }
        for (int i = 0; i <= srcLen; i++){
            matrix[i][0] = i*STEP;
        }
        for (int i = 0; i <= tarLen; i++){
            matrix[0][i] = i*STEP;
        }
        int dissimilarity;
        int[] srcInvalid = new int[srcLen], tarInvalid = new int[tarLen];
        for (int i = 0; i < srcLen; i++){
            for (int j = 0; j <tarLen; j++) {
                char src = source.charAt(i);
                char tag = target.charAt(j);
                if (src == tag) {
                    dissimilarity = 0;
                } else {
                    if (isNumber(src)){
                        dissimilarity = STEP;
                    } else if (isAlphabet(src)) {
                        if (caseInsensitive && isAlphabet(tag) && abs(src, tag)=='a'-'A'){
                            dissimilarity = (int) (STEP*0.1);
                        } else {
                            dissimilarity = STEP;
                        }
                    } else if (isBasicChinese(src)) {
                        if (samePronounce && isBasicChinese(tag)
                                && pinyinSame(PinyinHelper.toHanyuPinyinStringArray(src), PinyinHelper.toHanyuPinyinStringArray(tag))) {
                            dissimilarity = (int) (STEP * 0.3);
                        } else {
                            dissimilarity = STEP;
                        }
                    } else if (isSpecialChar(src)){
                        if (excludeSpecial) {
                            srcInvalid[i] = 1;
                            if (isSpecialChar(tag)) {
                                tarInvalid[j] = 1;
                                matrix[i+1][j+1] = matrix[i][j];
                            } else {
                                matrix[i+1][j+1] = matrix[i][j+1];
                            }
                            continue;
                        }
                        dissimilarity = STEP;
                    } else {
                        dissimilarity = STEP;
                    }
                    if (excludeSpecial && isSpecialChar(tag)) {
                        matrix[i+1][j+1] = matrix[i+1][j];
                        continue;
                    }
                }
                matrix[i+1][j+1] = min(matrix[i][j]+dissimilarity, matrix[i][j+1]+STEP, matrix[i+1][j]+STEP);
            }
        }
        int srcValidLen = srcLen - Arrays.stream(srcInvalid).sum();
        int tarValidLen = tarLen - Arrays.stream(tarInvalid).sum();
        if (srcValidLen <= MIN_LENGTH || tarValidLen <= MIN_LENGTH) {
            return 0;
        }
        return similarity(matrix[srcLen][tarLen], srcValidLen*STEP, tarValidLen*STEP);
    }

    private static double similarity(int distance, int srcLen, int tagLen) {
        if (srcLen==0){
            return 0;
        }
        return 1 - (double)distance/srcLen;
    }

    private static int min(int... array) {
        int min = Integer.MAX_VALUE;
        for (int i: array) {
            if (i<min){
                min = i;
            }
        }
        return min;
    }

    private static int abs(int left, int right) {
        return left>=right?left-right:right-left;
    }

    private static boolean pinyinSame(String[] src, String[] tag) {
        if (src == null || tag == null) return false;
        for (String s: src) {
            for (String t: tag) {
                if (s.equals(t)){
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isNumber(char c) {
        return c>'0' && c < '9';
    }

    private static boolean isAlphabet(char c) {
        return c>='A' && c <='z';
    }

    private static boolean isBasicChinese(char c) {
        return c >= BASIC_CHINESE[0] && c <= BASIC_CHINESE[1];
    }

    private static boolean isPunctuation(char c) {
        return (c >= 0x0020 && c <= 0x002F)
                || (c >= 0x003A && c <= 0x0040)
                || (c >= 0x005B && c <= 0x0060)
                || (c >= 0x007B && c <= 0x007F)
                ;
    }

    private static boolean isSpecialChar(char c) {
        return  !((c>=NUMBER_LATIN[0] && c <= NUMBER_LATIN[1]) || (c >= BASIC_CHINESE[0] && c <= BASIC_CHINESE[1]));
    }

}
