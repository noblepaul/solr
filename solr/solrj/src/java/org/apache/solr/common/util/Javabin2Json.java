/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class Javabin2Json {
    public static byte VERSION = 2;
    public byte tagByte;

    public byte[] bytes = new byte[1024];
    public char[] charArr = new char[1024];
    public int charArrEnd = 0;


    public  SimpleInputStream is;
    public  Writer os;
    public List<String> stringsList;

    public static final byte NULL = 0,
            BOOL_TRUE = 1,
            BOOL_FALSE = 2,
            BYTE = 3,
            SHORT = 4,
            DOUBLE = 5,
            INT = 6,
            LONG = 7,
            FLOAT = 8,
            DATE = 9,
            MAP = 10,
            SOLRDOC = 11,
            SOLRDOCLST = 12,
            ITERATOR = 14,
    /**
     * this is a special tag signals an end. No value is associated with it
     */
    END = 15,
            MAP_ENTRY_ITER = 17,
            UUID = 20, // This is reserved to be used only in LogCodec
    // types that combine tag + length (or other info) in a single byte
    TAG_AND_LEN = (byte) (1 << 5),
            STR = (byte) (1 << 5),
            SINT = (byte) (2 << 5),
            SLONG = (byte) (3 << 5),
            ARR = (byte) (4 << 5), //
            ORDERED_MAP = (byte) (5 << 5), // SimpleOrderedMap (a NamedList subclass, and more common)
            NAMED_LST = (byte) (6 << 5), // NamedList
            EXTERN_STRING = (byte) (7 << 5);

    public Javabin2Json(InputStream is, Writer os) {
        this.is = new SimpleInputStream(is);
        this.os = os;
    }

    public  boolean isContainer()  {
        switch (tagByte >>> 5) {
            case ARR >>> 5:
            case ORDERED_MAP >>> 5:
            case NAMED_LST >>> 5: return true;
        }

        switch (tagByte) {
            case MAP:
            case SOLRDOC:
            case SOLRDOCLST:
            case MAP_ENTRY_ITER:
            case ITERATOR: return true;
        }
        return false;
    }
    public boolean isKey()  {
        switch (tagByte >>> 5) {
            case EXTERN_STRING >>> 5:
            case STR >>> 5: return true;
        }
        switch (tagByte ) {case NULL: return true;}
        return false;
    }

    public void decode() throws IOException {
        int version = is.readByte();
        if (version != VERSION) {
            throw new RuntimeException(
                    "Invalid version (expected "
                            + VERSION
                            + ", but "
                            + version
                            + ") or the data in not in 'javabin' format");
        }

        tagByte = is.readByte();
        if (!isContainer()) throw new RuntimeException("Invalid Object type");
        readObject();
    }


    public String _readStr(int sz) {
        if(bytes.length<sz) {
            bytes = new byte[sz*2];
        }
        is.readFully(bytes, 0, sz);
        if(charArr.length < sz) charArr = new char[sz *2];
        charArrEnd = UTF8toUTF16(bytes, 0, sz, charArr, 0);
        return new String(charArr, 0, charArrEnd);
    }

    public int readSize() {
        int sz = tagByte & 0x1f;
        if (sz == 0x1f) sz += readVInt();
        return sz;
    }

    public int readVInt() {
        byte b = is.readByte();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = is.readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    public long readVLong() throws IOException {
        byte b = is.readByte();
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = is.readByte();
            i |= (long) (b & 0x7F) << shift;
        }
        return i;
    }



    public String readExternString() {
        int idx = readSize();
        if (idx != 0) { // idx != 0 is the index of the extern string
            String s = stringsList.get(idx - 1);
            writeJsonStr(s);
            return s;
        } else { // idx == 0 means it has a string value
            tagByte = is.readByte();
            String s = readStr();
            if (stringsList == null) stringsList = new ArrayList<>();
            stringsList.add(s);
            return s;
        }
    }

    public void writeJsonArr(int sz) throws IOException {
        os.append('[');
        for (int i = 0; sz == -1 || i < sz; i++) {
            if (i > 0) {
                os.write(" , ");
            }
            tagByte = is.readByte();
            if (tagByte == END) break;
            readObject();
        }
        os.append(']');
    }

    public void writeJsonObj(int sz) throws IOException {
        os.append('{');
        for (int i = 0; sz == -1 || i < sz; i++) {
            if (i > 0) {
                os.write(" , ");
            }
            tagByte = is.readByte();
            if(!isKey()) {
                throw  new RuntimeException("Not a valid Key");
            }
            if(tagByte == END && sz == -1) break;
            readObject();
            os.append(' ');
            os.append(':');
            os.append(' ');
            readVal();
        }
        os.append('}');
        os.append('\n');
    }


    public void readVal() throws IOException {
        tagByte = is.readByte();
        readObject();
    }

    public boolean writePrimitive() throws IOException {
        switch (tagByte) {
            case NULL: {
                os.write("null");
                return true;
            }
            case DATE: {
                os.write(String.valueOf(is.readLong()));
                return true;

            }
            case INT:
                os.write(is.readInt());
                return true;

            case BOOL_TRUE:
                os.write("true");
                return true;

            case BOOL_FALSE:
                os.write("false");
                return true;

            case FLOAT:
                os.write(String.valueOf(is.readFloat()));
                return true;

            case DOUBLE:
                os.write(String.valueOf(is.readDouble()));
                return true;

            case LONG:
                os.write(String.valueOf(is.readLong()));
                return true;

            case BYTE: {
                os.write(String.valueOf(is.readByte()));
                return true;
            }
            case SHORT: {
                os.write(String.valueOf(is.readShort()));
                return true;

            }
        }
        return false;
    }


    public void writeSmallInt() throws IOException {
        int v = tagByte & 0x0F;
        if ((tagByte & 0x10) != 0) v = (readVInt() << 4) | v;
        os.write(v);
    }

    public void writeLong() throws IOException {
        long v = tagByte & 0x0F;
        if ((tagByte & 0x10) != 0) v = (readVLong() << 4) | v;
        os.write(String.valueOf(v));
    }


    public void writeDocList() throws IOException {
        tagByte = is.readByte();
        int sz = readSize();
        for (int i = 0;  i < sz; i++) {
            readVal();
        }
        tagByte = is.readByte();
        writeJsonObj(readSize());
    }

    public  void readObject()  throws IOException{
        // if ((tagByte & 0xe0) == 0) {
        // if top 3 bits are clear, this is a normal tag

        // OK, try type + size in single byte
        switch (tagByte >>> 5) {
            case STR >>> 5: {
                readStr();
                return;
            }
            case SINT >>> 5: {
                writeSmallInt();
                return;
            }
            case SLONG >>> 5:
                writeLong();
                return;
            case ARR >>> 5: {
                writeJsonArr(readSize());
                return;
            }
            case ORDERED_MAP >>> 5:
            case NAMED_LST >>> 5: {
                writeJsonObj(readSize());
                return;
            }
            case EXTERN_STRING >>> 5: {
                readExternString();
                return;
            }
        }
        if(writePrimitive()) return;
        switch (tagByte) {
            case MAP: {
                writeJsonObj(readVInt());
                return;
            }
            case SOLRDOC:{
                tagByte = is.readByte();
                int size = readSize();
                writeJsonObj(size);

                return;}
            case SOLRDOCLST: {
                writeDocList();
                return;
            }
            case ITERATOR:
                writeJsonArr(-1);
                return;
            case END: return;
            case MAP_ENTRY_ITER: {
                writeJsonObj(-1);
                return;
            }
        }

        throw new RuntimeException("Unknown type " + tagByte);
    }
    void writeJsonStr(String s) {
        try {
            os.append('"');
            os.write(s);
            os.append('"');
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public String readStr()  {
        int sz = readSize();
        String s = _readStr(sz);
        writeJsonStr(s);
        return s;
    }

    public static int UTF8toUTF16(byte[] utf8, int offset, int len, char[] out, int out_offset) {
        int out_start = out_offset;
        final int limit = offset + len;
        while (offset < limit) {
            int b = utf8[offset++] & 0xff;

            if (b < 0xc0) {
                assert b < 0x80;
                out[out_offset++] = (char) b;
            } else if (b < 0xe0) {
                out[out_offset++] = (char) (((b & 0x1f) << 6) + (utf8[offset++] & 0x3f));
            } else if (b < 0xf0) {
                out[out_offset++] =
                        (char) (((b & 0xf) << 12) + ((utf8[offset] & 0x3f) << 6) + (utf8[offset + 1] & 0x3f));
                offset += 2;
            } else {
                assert b < 0xf8;
                int ch =
                        ((b & 0x7) << 18)
                                + ((utf8[offset] & 0x3f) << 12)
                                + ((utf8[offset + 1] & 0x3f) << 6)
                                + (utf8[offset + 2] & 0x3f);
                offset += 3;
                if (ch < 0xffff) {
                    out[out_offset++] = (char) ch;
                } else {
                    int chHalf = ch - 0x0010000;
                    out[out_offset++] = (char) ((chHalf >> 10) + 0xD800);
                    out[out_offset++] = (char) ((chHalf & 0x3FFL) + 0xDC00);
                }
            }
        }

        return out_offset - out_start;
    }

}
