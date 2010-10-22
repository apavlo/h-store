#!/usr/bin/python
# -*- coding: utf-8 -*-

"""A stupid message compiler for C++."""

import os.path
import re
import struct

def truncateLong(x):
    """Returns the uint32 part of a long x."""
    assert x > 0
    unsigned_bytes = struct.pack("<Q", x)
    return struct.unpack("<I", unsigned_bytes[:4])[0]


def reinterpretAsInt32(x):
    """Reinterprets an unsigned long as an int32."""
    assert x > 0
    unsigned_bytes = struct.pack("<Q", x)
    return struct.unpack("<i", unsigned_bytes[:4])[0]

class Type(object):
    INT32 = 0
    INT64 = 1
    BOOL = 2
    STRING = 3
    STRUCT = 4
    LIST = 5

    def __init__(self, code):
        assert Type.INT32 <= code and code <= Type.LIST
        self.typecode = code

    def typeCode(self):
        return self.typecode

INT32_MAX = 2147483647
INT32_MIN = -2147483648

INT32 = Type(Type.INT32)
INT64 = Type(Type.INT64)
BOOL = Type(Type.BOOL)
STRING = Type(Type.STRING)
STRUCT = Type(Type.STRUCT)

class List(Type):
    def __init__(self, subtype):
        Type.__init__(self, Type.LIST)
        self.subtype = subtype

    def typeCode(self):
        return truncateLong(self.typecode * 33) ^ self.subtype.typeCode()

class MessageDefinition(object):
    def __init__(self, name, comment):
        self.typecode = Type.STRUCT
        self.name = name
        self.fields = []
        self.comment = comment

    def addField(self, type_structure, name, default, comment):
        # Validate the default value
        if type_structure.typecode == Type.INT32:
            assert isinstance(default, int)
            assert INT32_MIN <= default and default <= INT32_MAX
        elif type_structure.typecode == Type.INT64:
            assert isinstance(default, int)
        elif type_structure.typecode == Type.BOOL:
            assert isinstance(default, bool)
        elif type_structure.typecode == Type.STRING:
            assert default is None or isinstance(default, str)
        elif type_structure.typecode == Type.STRUCT:
            assert default is None
        elif type_structure.typecode == Type.LIST:
            assert default is None or len(default) >= 0

        self.fields.append((type_structure, name, default, comment))

    def typeCode(self):
        """Generate a "unique" ID using a bad hash function."""
        # TODO: Do something smarter than this.

        def DJBHash(hashval, value):
            return truncateLong((hashval * 33) ^ value)

        def DJBStringHash(hashval, string):
            for c in string:
                hashval = DJBHash(hashval, ord(c))
            return hashval

        hashval = 5381
        hashval = DJBStringHash(hashval, self.name)

        for (type_structure, name, default, comment) in self.fields:
            hashval = DJBHash(hashval, type_structure.typeCode())
            hashval = DJBStringHash(hashval, name)

        return hashval


class CPPOutput(object):
    def __init__(self, filename, namespace):
        self.filename = filename
        self.namespace = namespace

        self.buffer = """/* AUTOMATICALLY GENERATED: DO NOT EDIT */
#ifndef %(guard)s
#define %(guard)s

#include <cassert>
#include <string>
#include <vector>

#include "base/assert.h"
#include "io/message.h"
#include "serialization.h"

namespace io {
class FIFOBuffer;
}

namespace %(namespace)s {\n\n""" % {'guard': self.includeGuard(), 'namespace': namespace}

        self.indent_ = 0
        self.loop_count_ = 0
        self.defined_types = {}

    def indent(self):
        self.indent_ += 4

    def unindent(self):
        self.indent_ -= 4
        assert self.indent_ >= 0

    def out(self, text):
        return " " * self.indent_ + text + "\n"

    def includeGuard(self):
        return self.filename.upper().replace(".", "_").replace("/", "_")

    def typeString(self, type_structure):
        if type_structure.typecode == Type.INT32:
            return "int32_t"
        if type_structure.typecode == Type.INT64:
            return "int64_t"
        if type_structure.typecode == Type.BOOL:
            return "bool"
        if type_structure.typecode == Type.STRING:
            return "std::string"
        if type_structure.typecode == Type.STRUCT:
            return type_structure.name
        if type_structure.typecode == Type.LIST:
            # Space before closing > to avoid >> for List(List(foo))
            return "std::vector<" + self.typeString(type_structure.subtype) + " >"
        raise ValueError, "Unknown type: " + str(type_structure)

    def typeValueString(self, type_structure, value):
        if type_structure.typecode == Type.INT32:
            return str(value)
        if type_structure.typecode == Type.INT64:
            return str(value)
        if type_structure.typecode == Type.BOOL:
            if value: return "true"
            return "false"
        if type_structure.typecode == Type.STRING:
            return "std::string"
        raise ValueError, "Unknown type: " + type_structure

    def serializeList(self, type_structure, name, serializeMethod):
            o = "{\n"

            self.indent()
            o += self.out("int32_t _size_ = static_cast<int32_t>(%s.size());" % (name))
            o += self.out(
                    "assert(0 <= _size_ && static_cast<size_t>(_size_) == %s.size());" % (name))
            o += self.out(self.serialize(INT32, "_size_"));

            # Ensure the loop index variable is unique for nested loops
            index_name = "_i%d_" % (self.loop_count_)
            self.loop_count_ += 1

            o += self.out("for (int %(index)s = 0; %(index)s < _size_; ++%(index)s) {" %
                    { "index": index_name })
            self.indent()
            o += self.out(serializeMethod(type_structure.subtype, name + "[" + index_name + "]"));
            self.unindent()

            o += self.out("}")
            self.unindent()
            o += self.out("}")
            # Trim trailing \n
            return o[:-1]

    def serialize(self, type_structure, name):
        if type_structure.typecode == Type.LIST:
            return self.serializeList(type_structure, name, self.serialize)
        elif type_structure.typecode == Type.STRUCT:
            return "%s.appendToString(_out_);" % (name)
        else:
            return "serialization::serialize(%s, _out_);" % (name)

    def serializeBuffer(self, type_structure, name):
        if type_structure.typecode == Type.LIST:
            return self.serializeList(type_structure, name, self.serializeBuffer)
        elif type_structure.typecode == Type.STRUCT:
            return "%s.serialize(_out_);" % (name)
        else:
            return "serialization::serialize(%s, _out_);" % (name)

    def deserialize(self, type_structure, name):
        if type_structure.typecode == Type.LIST:
            o = "{\n"
            self.indent()
            o += self.out("int32_t _size_;")
            o += self.out("_start_ = serialization::deserialize(&_size_, _start_, _end_);")
            o += self.out("assert(_size_ >= 0);")
            o += self.out("%s.resize(_size_);" % (name))

            # Ensure the loop index variable is unique for nested loops
            index_name = "_i%d_" % (self.loop_count_)
            self.loop_count_ += 1

            o += self.out("for (int %(index)s = 0; %(index)s < _size_; ++%(index)s) {" %
                    { "index": index_name })
            self.indent()
            o += self.out(self.deserialize(type_structure.subtype, name + "[" + index_name + "]"));
            self.unindent()
            o += self.out("}")
            self.unindent()
            o += self.out("}")
            # Trim trailing \n
            return o[:-1]
        elif type_structure.typecode == Type.STRUCT:
            return "_start_ = %s.parseFromString(_start_, _end_);" % (name)
        else:
            return "_start_ = serialization::deserialize(&%s, _start_, _end_);" % (name)

    def addMessage(self, message):
        self.buffer += self.out("// " + message.comment)
        self.buffer += self.out("class %s : public io::Message {" % (message.name))
        self.buffer += self.out("public:")
        self.indent()
        self.buffer += self.out("%s() :" % (message.name))

        # Initialize default types
        self.indent()
        for type_structure, name, default, comment in message.fields:
            # Skip field if there is no default value
            if default is None: continue

            self.buffer += self.out("%s(%s)," % (name, self.typeValueString(type_structure, default)))
        # Trim trailing ,
        if self.buffer.endswith(",\n"):
            self.buffer = self.buffer[:-2]
        else:
            assert self.buffer.endswith(" :\n")
            self.buffer = self.buffer[:-3]
        self.buffer += " {}\n"
        self.unindent()

        # Add field definitions
        for type_structure, name, default, comment in message.fields:
            self.buffer += "\n"
            if len(comment) > 0:
                self.buffer += self.out("// " + comment)
            self.buffer += self.out("%s %s;" % (self.typeString(type_structure), name))

        # Add operator== and !=. TODO: Move this to a .cc?
        self.buffer += "\n"
        self.buffer += self.out("bool operator==(const %s& other) const {" % message.name)
        self.indent()
        for type_structure, name, default, comment in message.fields:
            self.buffer += self.out("if (%s != other.%s) return false;" % (name, name))
        self.buffer += self.out("return true;")
        self.unindent()
        self.buffer += self.out("}")
        self.buffer += self.out("bool operator!=(const %s& other) const { return !(*this == other); }" % (message.name))

        # Add appendToString
        self.buffer += "\n"
        self.buffer += self.out("void appendToString(std::string* _out_) const {")
        self.indent()
        for type_structure, name, default, comment in message.fields:
            self.buffer += self.out(self.serialize(type_structure, name))
        self.unindent()
        self.buffer += self.out("}")

        # Add serialize
        self.buffer += "\n"
        self.buffer += self.out("virtual void serialize(io::FIFOBuffer* _out_) const {")
        self.indent()
        for type_structure, name, default, comment in message.fields:
            self.buffer += self.out(self.serializeBuffer(type_structure, name))
        self.unindent()
        self.buffer += self.out("}")

        # Add parseFromString
        self.buffer += "\n"
        self.buffer += self.out("const char* parseFromString(const char* _start_, const char* _end_) {")
        self.indent()
        for type_structure, name, default, comment in message.fields:
            self.buffer += self.out(self.deserialize(type_structure, name))
        self.buffer += self.out("return _start_;")
        self.unindent()
        self.buffer += self.out("}")
        # Add parseFromString(const std::string&) helper wrapper
        self.buffer += "\n"
        self.buffer += self.out("void parseFromString(const std::string& _str_) {")
        self.indent()
        self.buffer += self.out("const char* end = parseFromString(_str_.data(), _str_.data() + _str_.size());")
        self.buffer += self.out("ASSERT(end == _str_.data() + _str_.size());")
        self.unindent()
        self.buffer += self.out("}")

        # Add typeCode
        self.buffer += "\n"
        self.buffer += self.out("static int32_t typeCode() { return %d; }" % (
                reinterpretAsInt32(message.typeCode())))

        # All done
        self.unindent()
        self.buffer += self.out("};") + "\n"

    def output(self):
        self.buffer += "}  // namespace %s\n" % (self.namespace)
        self.buffer += "#endif  // %s\n" % (self.includeGuard())
        return self.buffer


class JavaOutput(object):
    def __init__(self, filename, namespace):
        self.namespace = namespace

        classname = os.path.splitext(os.path.basename(filename))[0]
        self.buffer = """/* AUTOMATICALLY GENERATED: DO NOT EDIT */

package %(namespace)s;

public final class %(classname)s {
    // Not constructible.
    private %(classname)s() {}
""" % {'namespace': namespace, 'classname': classname}

        self.indent_ = 4
        self.loop_count_ = 0
        self.defined_types = {}

    def indent(self):
        self.indent_ += 4

    def unindent(self):
        self.indent_ -= 4
        assert self.indent_ >= 0

    def out(self, text):
        return " " * self.indent_ + text + "\n"

    def referenceTypeString(self, type_structure):
        if type_structure.typecode == Type.INT32:
            return "Integer"
        if type_structure.typecode == Type.INT64:
            return "Long"
        if type_structure.typecode == Type.BOOL:
            return "Boolean"
        else:
            return self.typeString(type_structure)

    def typeString(self, type_structure):
        if type_structure.typecode == Type.INT32:
            return "int"
        if type_structure.typecode == Type.INT64:
            return "long"
        if type_structure.typecode == Type.BOOL:
            return "boolean"
        if type_structure.typecode == Type.STRING:
            return "String"
        if type_structure.typecode == Type.STRUCT:
            return type_structure.name
        if type_structure.typecode == Type.LIST:
            return "java.util.ArrayList<" + self.referenceTypeString(type_structure.subtype) + ">"
        raise ValueError, "Unknown type: " + str(type_structure)

    def typeValueString(self, type_structure, value):
        if type_structure.typecode == Type.INT32:
            if value is None: return "0"
            return str(value)
        if type_structure.typecode == Type.INT64:
            if value is None: return "0l"
            return str(value)
        if type_structure.typecode == Type.BOOL:
            if value: return "true"
            return "false"
        if type_structure.typecode == Type.STRING:
            if value is None: return '""'
            return '"' + value + '"'
        else:
            return "new " + self.typeString(type_structure) + "()"

    def serializeList(self, type_structure, name):
            o = self.serialize(INT32, name + ".size()") + "\n";

            # Ensure the loop variable is unique for nested loops
            index_name = "_i%d_" % (self.loop_count_)
            self.loop_count_ += 1

            o += self.out("for (%(type)s %(index)s : %(name)s) {" %
                    { "type": self.typeString(type_structure.subtype), "index": index_name, "name": name })
            self.indent()
            o += self.out(self.serialize(type_structure.subtype, index_name));
            self.unindent()

            o += self.out("}")
            # Trim trailing \n
            return o[:-1]

    def serialize(self, type_structure, name):
        if type_structure.typecode == Type.LIST:
            return self.serializeList(type_structure, name)
        elif type_structure.typecode == Type.STRUCT:
            return "%s.writeBytes(_out_);" % (name)
        else:
            return "com.relationalcloud.network.SerializationUtilities.writeBytes(%s, _out_);" % (name)

    def deserialize(self, type_structure, name):
        if type_structure.typecode == Type.LIST:
            # Make a unique size variable
            size_name = "_size%d_" % (self.loop_count_)
            o = self.deserialize(INT32, "int " + size_name) + "\n"
            #~ o = "int %s = com.relationalcloud.network.SerializationUtilities.readInt32(_in_);\n" % (size_name)
            o += self.out("assert %s >= 0;" % (size_name))
            o += self.out("%s.clear();" % (name))
            o += self.out("%s.ensureCapacity(%s);" % (name, size_name))

            # Ensure the temp variable is unique for nested loops
            temp_name = "_temp%d_" % (self.loop_count_)
            self.loop_count_ += 1

            o += self.out("while (%s > 0) {" % (size_name))
            self.indent()
            o += self.out("%s %s = %s;" % (self.typeString(type_structure.subtype), temp_name,
                    self.typeValueString(type_structure.subtype, None)));
            o += self.out(self.deserialize(type_structure.subtype, temp_name))
            o += self.out("%s.add(%s);" % (name, temp_name));
            o += self.out("%s -= 1;" % (size_name));
            self.unindent()
            o += self.out("}")
            # Trim trailing \n
            return o[:-1]
        elif type_structure.typecode == Type.STRUCT:
            return "if (!%s.readBytes(_in_)) return false;" % (name)
        elif type_structure.typecode == Type.STRING:
            o = "%s = com.relationalcloud.network.SerializationUtilities.readString(_in_);\n" % (name)
            o += self.out("if (%s == null) return false;" % (name))
            return o[:-1]
        elif type_structure.typecode == Type.INT32:
            temp_buffer = "_buffer%d_" % (self.loop_count_)
            self.loop_count_ += 1
            o = "byte[] %s = com.relationalcloud.network.SerializationUtilities.readBytes(4, _in_);\n" % (temp_buffer)
            o += self.out("if (%s == null) return false;" % (temp_buffer));
            o += self.out("%s = com.relationalcloud.network.SerializationUtilities.fromLittleEndianInt32(%s);" % (name, temp_buffer));
            return o[:-1]
        else:
            raise ValueError("TODO " + type_structure)

    def javaName(self, name):
        # Convert C++ style names into Java style names
        CPP_NAME_RE = re.compile("_([a-z])")
        def stripUpper(matchobj):
            return matchobj.group(1).upper()
        return CPP_NAME_RE.sub(stripUpper, name)

    def addMessage(self, message):
        self.buffer += "\n"
        self.buffer += self.out("/** " + message.comment + "*/")
        self.buffer += self.out("public static final class %s {" % (message.name))
        self.indent()

        # Add field definitions
        for type_structure, name, default, comment in message.fields:
            self.buffer += "\n"
            if len(comment) > 0:
                self.buffer += self.out("/** " + comment + " */")
            self.buffer += self.out("public %s %s = %s;" % (self.typeString(type_structure), self.javaName(name), self.typeValueString(type_structure, default)))

        # Add toBytes
        self.buffer += "\n"
        self.buffer += self.out("/** Writes the serialized message to the OutputStream. */")
        self.buffer += self.out("public void writeBytes(java.io.OutputStream _out_) {")
        self.indent()
        self.buffer += self.out("try {")
        self.indent();
        for type_structure, name, default, comment in message.fields:
            self.buffer += self.out(self.serialize(type_structure, self.javaName(name)))
        self.unindent()
        self.buffer += self.out("} catch (java.io.IOException e) {")
        self.indent();
        self.buffer += self.out("throw new RuntimeException(e);")
        self.unindent();
        self.buffer += self.out("}")
        self.unindent();
        self.buffer += self.out("}")

        # Add fromBytes
        self.buffer += "\n"
        self.buffer += self.out("/** Reads the serialized values from the InputStream. */")
        self.buffer += self.out("public boolean readBytes(java.io.InputStream _in_) {")
        self.indent()
        self.buffer += self.out("try {")
        self.indent();
        for type_structure, name, default, comment in message.fields:
            self.buffer += self.out(self.deserialize(type_structure, self.javaName(name)))
        self.unindent()
        self.buffer += self.out("} catch (java.io.IOException e) {")
        self.indent();
        self.buffer += self.out("throw new RuntimeException(e);")
        self.unindent();
        self.buffer += self.out("}")
        self.buffer += self.out("return true;")
        self.unindent()
        self.buffer += self.out("}")

        # Add typeCode
        self.buffer += "\n"
        self.buffer += self.out("/** Unique identifier for this message type. */");
        self.buffer += self.out("public static int TYPE_CODE = %d;" % (
                reinterpretAsInt32(message.typeCode())))

        # All done
        self.unindent()
        self.buffer += self.out("}")

    def output(self):
        self.buffer += "}\n"
        return self.buffer


def main(messages, namespace):
    import sys

    if len(sys.argv) != 2:
        sys.stderr.write("%s [output .h]\n" % (sys.argv[0]))
        sys.exit(1)

    output_path = sys.argv[1]
    if output_path.endswith('.h'):
        out = CPPOutput(output_path, namespace)
    elif output_path.endswith('.java'):
        out = JavaOutput(output_path, namespace)
    else:
        sys.stderr.write("unsupported output type (try .h or .java)\n")
        sys.exit(1)

    for message in messages:
        out.addMessage(message)

    f = open(output_path, "w")
    f.write(out.output())
    f.close()
