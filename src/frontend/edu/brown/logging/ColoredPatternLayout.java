package edu.brown.logging;

/*
 * Copyright  2002,2004 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.Level;

import java.util.StringTokenizer;

/**
 * <p> A layout wich has the same functions as the 
 *     Patternlayout.</p>
 * <p> But it also allows to let the output be colored</p>
 * <p> The ColorStrings are formated as follows <br>
 *     "foreground/background/attribute" where
 *      <ul>
 *        <li> foreground and backround is one of
 *         <ul>
 *          <li>black</li>
 *          <li>red</li>
 *          <li>green</li>
 *          <li>yellow</li>
 *          <li>blue</li>
 *          <li>magenta</li>
 *          <li>cyan</li>
 *          <li>white</li>
 *         </ul>
 *        </li>
 *        <li> attribute is one of
 *         <ul>
 *          <li>normal</li><li>bright</li><li>dim</li>
 *          <li>underline</li><li>blink</li><li>reverse</li>
 *          <li>hidden</li>
 *         </ul>
 *        </li>
 *      </li>
 *     </ul>
 * </p>
 * <p>
 *   the following colors could be set
 *   <ul>
 *     <li>FatalErrorColor</li>
 *     <li>ErrorColor</li>
 *     <li>WarnColor</li>
 *     <li>InfoColor</li>
 *     <li>DebugColor</li>
 *   </ul>
 * </p>
 *  
 * major parts of this class came from the apache ant AnsiColorLogger
 * @author Ingo Thon isthon a-t gmx d0t de
 * @version 0.1 (alpha)
 */
public class ColoredPatternLayout extends PatternLayout{
    private static final String CONFIGURATION_SEPARATOR="/";

    private static final String PREFIX = "\u001b[";
    private static final String SUFFIX = "m";
    private static final char SEPARATOR = ';';
    private static final String END_COLOR = PREFIX + SUFFIX;

    private static final String ATTR_NORMAL = "0";
    private static final String ATTR_BRIGHT = "1";
    private static final String ATTR_DIM = "2";
    private static final String ATTR_UNDERLINE = "3";
    private static final String ATTR_BLINK = "5";
    private static final String ATTR_REVERSE = "7";
    private static final String ATTR_HIDDEN = "8";

    private static final String FG_BLACK = "30";
    private static final String FG_RED = "31";
    private static final String FG_GREEN = "32";
    private static final String FG_YELLOW = "33";
    private static final String FG_BLUE = "34";
    private static final String FG_MAGENTA = "35";
    private static final String FG_CYAN = "36";
    private static final String FG_WHITE = "37";

    private static final String BG_BLACK = "40";
    private static final String BG_RED = "41";
    private static final String BG_GREEN = "42";
    private static final String BG_YELLOW = "44";
    private static final String BG_BLUE = "44";
    private static final String BG_MAGENTA = "45";
    private static final String BG_CYAN = "46";
    private static final String BG_WHITE = "47";

    private String fatalErrorColor
    = PREFIX + ATTR_DIM + SEPARATOR + FG_RED + SUFFIX;
    private String errorColor
    = PREFIX + ATTR_DIM + SEPARATOR + FG_RED + SUFFIX;
    private String warnColor
    = PREFIX + ATTR_DIM + SEPARATOR + FG_MAGENTA + SUFFIX;
    private String infoColor
    = PREFIX + ATTR_DIM + SEPARATOR + FG_GREEN + SUFFIX;
    private String debugColor
    = PREFIX + ATTR_DIM + SEPARATOR + FG_WHITE + SUFFIX;
    
    public ColoredPatternLayout(){
    super();
    }
    public ColoredPatternLayout(String pattern){
    super(pattern);
    }

    public String format(LoggingEvent event){
    if( event.getLevel() == Level.FATAL){
        return fatalErrorColor+super.format(event)+END_COLOR;
    }else if( event.getLevel() == Level.ERROR){
        return errorColor+super.format(event)+END_COLOR;
    }if( event.getLevel() == Level.WARN){
        return super.format(event);
//        return warnColor+super.format(event)+END_COLOR;
    }if( event.getLevel() == Level.INFO){
        return super.format(event);
//        return infoColor+super.format(event)+END_COLOR;
    }if( event.getLevel() == Level.DEBUG){
        return super.format(event);
//        return debugColor+super.format(event)+END_COLOR;
    }else{
        throw new RuntimeException("Unsupported Level "+event.toString());
    }
    }
    private String makeFgString(String fgColorName){
    //System.out.println("fg "+fgColorName);
    if(fgColorName.toLowerCase().equals("black")){
        return FG_BLACK;
    }else if(fgColorName.toLowerCase().equals("red")){
        return FG_RED;
    }else if(fgColorName.toLowerCase().equals("green")){
        return FG_GREEN;
    }else if(fgColorName.toLowerCase().equals("yellow")){
        return FG_YELLOW;
    }else if(fgColorName.toLowerCase().equals("blue")){
        return FG_BLUE;
    }else if(fgColorName.toLowerCase().equals("magenta")){
        return FG_MAGENTA;
    }else if(fgColorName.toLowerCase().equals("cyan")){
        return FG_CYAN;
    }else if(fgColorName.toLowerCase().equals("white")){
        return FG_WHITE;
    }
    System.out.println("log4j: ColoredPatternLayout Unsupported FgColor "+fgColorName);
    return "-1";
    }
    private String makeBgString(String bgColorName){
    //System.out.println("bg "+bgColorName);
    if(bgColorName.toLowerCase().equals("black")){
        return BG_BLACK;
    }else if(bgColorName.toLowerCase().equals("red")){
        return BG_RED;
    }else if(bgColorName.toLowerCase().equals("green")){
        return BG_GREEN;
    }else if(bgColorName.toLowerCase().equals("yellow")){
        return BG_YELLOW;
    }else if(bgColorName.toLowerCase().equals("blue")){
        return BG_BLUE;
    }else if(bgColorName.toLowerCase().equals("magenta")){
        return BG_MAGENTA;
    }else if(bgColorName.toLowerCase().equals("cyan")){
        return BG_CYAN;
    }else if(bgColorName.toLowerCase().equals("white")){
        return BG_WHITE;
    }
    System.out.println("log4j: ColoredPatternLayout Unsupported BgColor "+bgColorName);
    return "-1";
    }


    private String makeAttributeString(String attributeName){
    //System.out.println("attribute "+attributeName);
    if(attributeName.toLowerCase().equals("normal")){
        return ATTR_NORMAL;
    }else if(attributeName.toLowerCase().equals("bright")){
        return ATTR_BRIGHT;
    }else if(attributeName.toLowerCase().equals("dim")){
        return ATTR_DIM;
    }else if(attributeName.toLowerCase().equals("underline")){
        return ATTR_UNDERLINE;
    }else if(attributeName.toLowerCase().equals("blink")){
        return ATTR_BLINK;
    }else if(attributeName.toLowerCase().equals("reverse")){
        return ATTR_REVERSE;
    }else if(attributeName.toLowerCase().equals("hidden")){
        return ATTR_HIDDEN;
    }
    System.out.println("log4j: ColoredPatternLayout Unsupported Attribute "+attributeName);

    return "-1";
    }

    private String makeColorString(String colorName){
    //System.out.println(colorName);
    StringTokenizer tokenizer=new StringTokenizer(colorName,
                              CONFIGURATION_SEPARATOR);
    String fgColor=FG_WHITE;
    String bgColor=BG_BLACK;
    String attr=ATTR_NORMAL;
    if(!tokenizer.hasMoreTokens()){
        return PREFIX + ATTR_NORMAL + SEPARATOR + FG_WHITE + SUFFIX;
    }
    if(tokenizer.hasMoreTokens()){
        fgColor=makeFgString(tokenizer.nextToken());
    }
    if(tokenizer.hasMoreTokens()){
        bgColor=makeBgString(tokenizer.nextToken());
    }
    if(tokenizer.hasMoreTokens()){
        attr=makeAttributeString(tokenizer.nextToken());
    }
    //return PREFIX + ATTR_DIM + SEPARATOR + FG_WHITE + SUFFIX;
    return PREFIX + attr
        + SEPARATOR + fgColor
        + SEPARATOR + bgColor
        +SUFFIX;
    }
    //--> from here configuration {
    /** returns the configured color for error msgs 
     * should normally return only the string for the color
     */
    public String getFatalErrorColor(){
    return fatalErrorColor;
    }
    public void setFatalErrorColor(String colorName){
    fatalErrorColor=makeColorString(colorName);
    }
    /** returns the configured color for error msgs 
     * should normally return only the string for the color
     */
    public String getErrorColor(){
    return errorColor;
    }
    public void setErrorColor(String colorName){
    //System.out.println(colorName);
    errorColor=makeColorString(colorName);
    }
    /** returns the configured color for error msgs 
     * should normally return only the string for the color
     */
    public String getWarnColor(){
    return warnColor;
    }
    public void setWarnColor(String colorName){
    warnColor=makeColorString(colorName);
    }
    /** returns the configured color for error msgs 
     * should normally return only the string for the color
     */
    public String getInfoColor(){
    return infoColor;
    }
    public void setInfoColor(String colorName){
    infoColor=makeColorString(colorName);
    }
    /** returns the configured color for error msgs 
     * should normally return only the string for the color
     */
    public String getDebugColor(){
    return debugColor;
    }
    public void setDebugColor(String colorName){
    //System.out.println(makeColorString(colorName));
    debugColor=makeColorString(colorName);
    }

    //<-- configuration }


}