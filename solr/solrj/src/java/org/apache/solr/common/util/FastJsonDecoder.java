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
import java.io.Reader;

import org.noggit.JSONParser;

import static org.apache.solr.common.util.DataEntry.Type.KEYVAL_ITER;
import static org.apache.solr.common.util.DataEntry.Type.STR;
import static org.noggit.JSONParser.ARRAY_START;
import static org.noggit.JSONParser.BOOLEAN;
import static org.noggit.JSONParser.EOF;
import static org.noggit.JSONParser.LONG;
import static org.noggit.JSONParser.NULL;
import static org.noggit.JSONParser.NUMBER;
import static org.noggit.JSONParser.OBJECT_END;
import static org.noggit.JSONParser.OBJECT_START;
import static org.noggit.JSONParser.STRING;

public class FastJsonDecoder {
  private JSONParser parser;
  private DataEntry.EntryListener rootListener;
  private final EntryImpl root = new EntryImpl(null);

  public FastJsonDecoder() {
    root.listener = e -> e.listenContainer(null, rootListener);
  }


  public FastJsonDecoder withReader(Reader is) {
    parser = new JSONParser(is);
    return this;
  }

  public FastJsonDecoder withListener(DataEntry.EntryListener listener) {
    rootListener = listener;
    return this;
  }


  public Object parse() throws IOException {
    int event = -1;
    for (; ; ) {
      event = parser.nextEvent();
      if (event == EOF) break;
      if (event == OBJECT_START) {
        root.handleJsonObj();
      } else if (event == ARRAY_START) {
        throw new RuntimeException("Array Unsupported");
      }
    }
    return root.child != null ?
        root.child.ctx :
        null;
  }

  public class EntryImpl implements DataEntry {
    Object val;
    long longNum;
    double doubleNum;
    boolean bool;
    DataEntry.Type typ;
    String name;
    EntryListener listener;
    Object ctx;
    final int depth;

    int idx = -1;

    final EntryImpl parent;
    EntryImpl child;


    EntryImpl(EntryImpl parent) {
      this.parent = parent;
      reset();
      if (parent != null) {
        depth = parent.depth + 1;
      } else {
        depth = -1;
      }
    }

    @Override
    public Type type() {
      return typ;
    }

    @Override
    public long index() {
      return idx;
    }

    @Override
    public int intVal() {
      if (typ == Type.LONG) {
        return (int) longNum;
      } else if (typ == Type.DOUBLE) {
        return (int) doubleNum;
      } else if (typ == STR) {
        return Integer.parseInt(val.toString());
      } else {
        throw new NumberFormatException("Unknown type");
      }
    }

    @Override
    public long longVal() {
      if (typ == Type.LONG) {
        return longNum;
      } else if (typ == Type.DOUBLE) {
        return (long) doubleNum;
      } else if (typ == STR) {
        return Long.parseLong(val.toString());
      } else {
        throw new NumberFormatException("Unknown type");
      }
    }

    @Override
    public float floatVal() {
      if (typ == Type.LONG) {
        return (float) longNum;
      } else if (typ == Type.DOUBLE) {
        return (float) doubleNum;
      } else if (typ == STR) {
        return Float.parseFloat(val.toString());
      } else {
        throw new NumberFormatException("Unknown type");
      }
    }

    @Override
    public double doubleVal() {
      if (typ == Type.LONG) {
        return (double) longNum;
      } else if (typ == Type.DOUBLE) {
        return doubleNum;
      } else if (typ == STR) {
        return Double.parseDouble(val.toString());
      } else {
        throw new NumberFormatException("Unknown type");
      }
    }

    @Override
    public boolean boolVal() {
      if (typ == Type.BOOL) {
        return bool;
      } else if (typ == STR) {
        return Boolean.parseBoolean(val.toString());
      } else {
        throw new NumberFormatException("Unknown type");
      }

    }

    @Override
    public Object val() {
      if (typ == Type.LONG) {
        return longNum;
      } else if (typ == Type.DOUBLE) {
        return doubleNum;
      } else if (typ == Type.NULL) {
        return null;
      } else if (typ == Type.BOOL) {
        return bool;
      } else {
        return val;
      }
    }

    @Override
    public void listenContainer(Object ctx, EntryListener listener) {
      this.listener = listener;
      this.ctx = ctx;
    }

    @Override
    public Object metadata() {
      return null;
    }

    @Override
    public int depth() {
      return depth;
    }

    @Override
    public DataEntry parent() {
      return parent;
    }

    @Override
    public Object ctx() {
      return ctx;
    }

    @Override
    public int length() {
      return -1;
    }

    @Override
    public boolean isKeyValEntry() {
      return typ == KEYVAL_ITER;
    }

    @Override
    public CharSequence name() {
      return name;
    }

    public void reset() {
      idx = -1;
      this.doubleNum = 0.0;
      this.longNum = 0L;
      this.bool = false;
      this.val = null;
      this.typ = null;
    }


    EntryImpl getChild() {
      if (child == null) {
        child = new EntryImpl(this);
      }
      child.reset();
      return child;
    }

    void handleJsonObj() throws IOException {
      typ = DataEntry.Type.KEYVAL_ITER;
      EntryImpl child = initChildObj();
      if (child == null) return;
      child.forEachAttr();
    }

    private EntryImpl initChildObj() throws IOException {
      EntryListener oldListener = this.listener;
      Object oldCtx = this.ctx;
      this.listener = null;
      oldListener.entry(this);
      EntryListener nextListener = this.listener;
      Object nextCtx = this.ctx;
      this.listener = oldListener;
      this.ctx = oldCtx;
      if (nextListener == null) {
        if (typ == KEYVAL_ITER) {
          JsonRecordReader.consumeTillMatchingEnd(parser, 1, 0);
        } else {
          JsonRecordReader.consumeTillMatchingEnd(parser, 0, 1);
        }
        return null;
      }
      EntryImpl c = getChild();
      c.listener = nextListener;
      c.ctx = nextCtx;
      return c;
    }

    private void forEachAttr() throws IOException {
      for (; ; ) {
        int event = parser.nextEvent();
        if (event == OBJECT_END) {
          endContainer();
          return;
        }
        assert event == STRING;
        assert parser.wasKey();
        name = parser.getString();
        idx++;
        if (handleOneEntry()) return;
      }
    }

    private boolean handleOneEntry() throws IOException {
      int event = parser.nextEvent();
      if (handlePrimitive(event)) {
        if (this.listener == null) {
          //skip the rest of the object
          JsonRecordReader.consumeTillMatchingEnd(parser, 1, 0);
          return true;
        }
      } else if (event == OBJECT_START) {
        handleJsonObj();
      } else if (event == ARRAY_START) {
        //todo
      }
      return false;
    }

    private void endContainer() {
      if (listener != null) {
        idx = -1;
        name = null;
        typ = null;
        listener.end(this);
      }
    }


    private boolean handlePrimitive(int ev) throws IOException {
      switch (ev) {
        case STRING:
          this.typ = STR;
          this.val = parser.getString();
          break;
        case LONG:
          this.typ = DataEntry.Type.LONG;
          this.val = parser.getLong();
          break;
        case NUMBER:
          this.typ = DataEntry.Type.DOUBLE;
          this.val = parser.getDouble();
          break;
        case BOOLEAN:
          this.typ = DataEntry.Type.BOOL;
          this.val = parser.getBoolean();
          break;
        case NULL:
          this.typ = DataEntry.Type.NULL;
          parser.getNull();
          break;
        default:
          return false;
      }
      listener.entry(this);
      return true;
    }
  }
}
