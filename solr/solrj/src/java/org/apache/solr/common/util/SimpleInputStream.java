package org.apache.solr.common.util;

import java.io.InputStream;

public class SimpleInputStream {

    public final InputStream in;
    public byte[] buf = new byte[1024 * 8];
    public int pos = 0;
    public int end = 0;
    public long readFromStream;

    public SimpleInputStream(InputStream in) {
        this.in = in;
    }
    public byte readByte()  {
        if (pos >= end) {
            refill();
            if (pos >= end) throw new RuntimeException();
        }
        return buf[pos++];
    }
    public short readShort()  {
        return (short) ((readUnsignedByte() << 8) | readUnsignedByte());
    }


    public int readInt()  {
        return ((readUnsignedByte() << 24)
                | (readUnsignedByte() << 16)
                | (readUnsignedByte() << 8)
                | readUnsignedByte());
    }

    public int read()  {
        if (pos >= end) {
            refill();
            if (pos >= end) return -1;
        }
        return buf[pos++] & 0xff;
    }
    public void refill()  {
        // this will set end to -1 at EOF
        end = readWrappedStream(buf, 0, buf.length);
        if (end > 0) readFromStream += end;
        pos = 0;
    }


    public int readWrappedStream(byte[] target, int offset, int len)  {
        if (in == null) return -1;
        try {
            return in.read(target, offset, len);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void readFully(byte b[])  {
        readFully(b, 0, b.length);
    }
    public int read(byte b[], int off, int len)  {
        int r = 0; // number of bytes we have read

        // first read from our buffer;
        if (end - pos > 0) {
            r = Math.min(end - pos, len);
            System.arraycopy(buf, pos, b, off, r);
            pos += r;
        }

        if (r == len) return r;

        // amount left to read is >= buffer size
        if (len - r >= buf.length) {
            int ret = readWrappedStream(b, off + r, len - r);
            if (ret >= 0) {
                readFromStream += ret;
                r += ret;
                return r;
            } else {
                // negative return code
                return r > 0 ? r : -1;
            }
        }

        refill();

        // read rest from our buffer
        if (end - pos > 0) {
            int toRead = Math.min(end - pos, len - r);
            System.arraycopy(buf, pos, b, off + r, toRead);
            pos += toRead;
            r += toRead;
            return r;
        }

        return r > 0 ? r : -1;
    }



    public long readLong()  {
        return (((long) readUnsignedByte()) << 56)
                | (((long) readUnsignedByte()) << 48)
                | (((long) readUnsignedByte()) << 40)
                | (((long) readUnsignedByte()) << 32)
                | (((long) readUnsignedByte()) << 24)
                | (readUnsignedByte() << 16)
                | (readUnsignedByte() << 8)
                | (readUnsignedByte());
    }

    public float readFloat()  {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble()  {
        return Double.longBitsToDouble(readLong());
    }

    public int readUnsignedByte()  {
        if (pos >= end) {
            refill();
            if (pos >= end) {
                throw new RuntimeException();
            }
        }
        return buf[pos++] & 0xff;
    }

    public void readFully(byte b[], int off, int len)  {
        while (len > 0) {
            int ret = read(b, off, len);
            if (ret == -1) {
                throw new RuntimeException();
            }
            off += ret;
            len -= ret;
        }
    }



}
