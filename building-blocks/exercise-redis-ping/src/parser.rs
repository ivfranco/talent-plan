use super::{Error, Result};
use serde::ser::SerializeSeq;
use serde::{
    de::{SeqAccess, Visitor},
    ser::{
        Impossible, SerializeStruct, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
    },
    Deserializer,
};
use std::{
    convert::TryInto,
    io::{BufRead, BufReader, ErrorKind, Read, Write},
    str::from_utf8,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    SimpleString(String),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Value>),
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_unit(),
            Value::SimpleString(s) => serializer.serialize_str(s),
            Value::BulkString(b) => serializer.serialize_bytes(b),
            Value::Integer(i) => serializer.serialize_i64(*i),
            Value::Array(a) => {
                let mut seq = serializer.serialize_seq(Some(a.len()))?;
                for v in a {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
        }
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "A RESP value")
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::BulkString(v))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::SimpleString(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::SimpleString(v))
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Integer(v))
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = vec![];
        while let Some(v) = seq.next_element()? {
            vec.push(v);
        }

        Ok(Value::Array(vec))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null)
    }
}

impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

pub struct Serializer<W> {
    writer: W,
}

impl<W: Write> Serializer<W> {
    pub fn from_writer(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write> serde::Serializer for &mut Serializer<W> {
    type Ok = ();

    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        write!(&mut self.writer, ":{}\r\n", v)?;
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok> {
        Err(Error::Unsupported)
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok> {
        Err(Error::Unsupported)
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok> {
        Err(Error::Unsupported)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        self.serialize_str(&v.to_string())
    }

    // str serializes to simple string
    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        if v.contains('\r') || v.contains('\n') {
            Err(Error::NonUtf8)
        } else {
            write!(&mut self.writer, "+{}\r\n", v)?;
            Ok(())
        }
    }

    // bytes serializes to bulk string
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        let len = v.len();
        write!(&mut self.writer, "${}\r\n", len)?;
        self.writer.write_all(v)?;
        write!(&mut self.writer, "\r\n")?;
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        write!(&mut self.writer, "$-1\r\n")?;
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        Err(Error::Unsupported)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(self, _name: &'static str, value: &T) -> Result<Self::Ok>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: serde::Serialize,
    {
        self.serialize_str(variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if let Some(len) = len {
            write!(&mut self.writer, "*{}\r\n", len)?;
            Ok(self)
        } else {
            Err(Error::Serde("Array of unknown length".to_string()))
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        // RESP doesn't distinguish between tuple and homogeneous sequence
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        // a singleton Map type backported from RESPv3
        write!(&mut self.writer, "%1\r\n")?;
        self.serialize_str(variant)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::Unsupported)
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        // backported from RESPv3
        write!(&mut self.writer, "%{}\r\n", len)?;
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(Error::Unsupported)
    }
}

impl<W: Write> SerializeSeq for &mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<W: Write> SerializeTuple for &mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<W: Write> SerializeTupleStruct for &mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<W: Write> SerializeTupleVariant for &mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<W: Write> SerializeStruct for &mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        use serde::Serializer;

        (**self).serialize_str(key)?;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

#[allow(unused_variables)]
impl<'de, R: BufRead> serde::Deserializer<'de> for &mut Parser<R> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.peek()? {
            b'+' => self.deserialize_string(visitor),
            b':' => self.deserialize_i64(visitor),
            b'$' => self.deserialize_byte_buf(visitor),
            b'*' => self.deserialize_seq(visitor),
            _ => Err(Error::Unsupported),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.integer()? {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            _ => Err(Error::Overflow),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_i8(int)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_i16(int)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_i32(int)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_i64(self.integer()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_u8(int)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_u16(int)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_u32(int)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.integer()?.try_into().map_err(|_| Error::Overflow)?;
        visitor.visit_u64(int)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Unsupported)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Unsupported)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let str = self.simple_string()?;
        let mut chars = str.chars();
        let char = chars
            .next()
            .ok_or_else(|| Error::Serde("Expected char, found empty string".to_string()))?;
        if chars.next().is_none() {
            visitor.visit_char(char)
        } else {
            Err(Error::Serde(
                "Expacted char, found multi-char string".to_string(),
            ))
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_string(self.simple_string()?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Some(bytes) = self.bulk_string()? {
            visitor.visit_byte_buf(bytes)
        } else {
            visitor.visit_unit()
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.tag(b"*")?;
        let len = self.signed()?;

        if len < 0 {
            return visitor.visit_unit();
        }

        self.crlf()?;
        visitor.visit_seq(Counted {
            parser: self,
            count: len as usize,
        })
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}

struct Counted<'a, R> {
    parser: &'a mut Parser<R>,
    count: usize,
}

impl<'a, 'de, R: BufRead> SeqAccess<'de> for Counted<'a, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.count == 0 {
            Ok(None)
        } else {
            self.count -= 1;

            // this triggers recursive type bound E0275
            // seed.deserialize(&mut Parser::from_buffered(&mut self.parser.reader))
            //     .map(Some)

            // this triggers compile error "cannot move out value behind a
            // mutable reference"
            // seed.deserialize(self.parser).map(Some)

            // this is fine, &mut * creates a new mutable reference to Parser
            // lexically overlapping &mut self but legal according to NLL
            seed.deserialize(&mut *self.parser).map(Some)
        }
    }
}

pub struct Parser<R> {
    reader: R,
}

impl<R: Read> Parser<BufReader<R>> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }
}

impl<R: BufRead> Parser<R> {
    pub fn from_buffered(reader: R) -> Self {
        Self { reader }
    }

    fn peek(&mut self) -> Result<u8> {
        self.buffer().map(|buf| buf[0])
    }

    fn tag(&mut self, bytes: &[u8]) -> Result<()> {
        let mut buf = vec![0; bytes.len()];
        self.reader.read_exact(&mut buf)?;
        if buf == bytes {
            Ok(())
        } else {
            Err(Error::SyntaxError(format!(
                "Expected {:x?}, Found {:x?}",
                bytes, buf
            )))
        }
    }

    fn crlf(&mut self) -> Result<()> {
        self.tag(b"\r\n")
    }

    fn buffer(&mut self) -> Result<&[u8]> {
        let buf = self.reader.fill_buf()?;

        if buf.is_empty() {
            Err(Error::IO(std::io::Error::from(ErrorKind::UnexpectedEof)))
        } else {
            Ok(buf)
        }
    }

    fn take_while<F>(&mut self, mut f: F) -> Result<Vec<u8>>
    where
        F: FnMut(&u8) -> bool,
    {
        let mut vec = vec![];
        loop {
            let buf = self.buffer()?;
            let amt = buf.iter().position(|b| !f(b)).unwrap_or(buf.len());
            let (prefix, suffix) = buf.split_at(amt);
            vec.extend_from_slice(prefix);
            let consumed = suffix.is_empty();

            self.reader.consume(amt);
            if !consumed {
                return Ok(vec);
            }
        }
    }

    fn unsigned(&mut self) -> Result<i64> {
        let digits = self.take_while(u8::is_ascii_digit)?;
        digits
            .into_iter()
            .try_fold(0i64, |int, b| {
                let int = int.checked_mul(10)?;
                int.checked_add(i64::from(b - b'0'))
            })
            .ok_or(Error::Overflow)
    }

    fn signed(&mut self) -> Result<i64> {
        let buf = self.buffer()?;

        let neg = buf.starts_with(b"-");
        if neg {
            self.reader.consume(1);
        }

        let int = self.unsigned()?;

        if neg {
            int.checked_neg().ok_or(Error::Overflow)
        } else {
            Ok(int)
        }
    }

    pub fn simple_string(&mut self) -> Result<String> {
        self.tag(b"+")?;
        let str = self.take_while(|&b| b != b'\r' && b != b'\n')?;
        self.crlf()?;
        from_utf8(&str)
            .map(|s| s.to_string())
            .map_err(|_| Error::NonUtf8)
    }

    pub fn integer(&mut self) -> Result<i64> {
        self.tag(b":")?;
        let int = self.signed()?;
        self.crlf()?;
        Ok(int)
    }

    pub fn bulk_string(&mut self) -> Result<Option<Vec<u8>>> {
        self.tag(b"$")?;
        let len = self.signed()?;
        self.crlf()?;

        if len < 0 {
            Ok(None)
        } else {
            let mut str = vec![0; len as usize];
            self.reader.read_exact(&mut str)?;
            self.crlf()?;
            Ok(Some(str))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn parse_integer() -> Result<()> {
        let mut parser = Parser::new(b":-11313\r\n".as_ref());
        assert_eq!(parser.integer()?, -11313);
        Ok(())
    }

    #[test]
    fn parse_simple_string() -> Result<()> {
        let mut parser = Parser::new(b"+This is a test\r\n".as_ref());
        assert_eq!(parser.simple_string()?, "This is a test");
        Ok(())
    }

    #[test]
    fn parse_bulk_string() -> Result<()> {
        let mut parser = Parser::new(b"$10\r\nHelloWorld\r\n".as_ref());
        assert_eq!(parser.bulk_string()?, Some(b"HelloWorld".to_vec()));
        Ok(())
    }

    #[test]
    fn parse_null_string() -> Result<()> {
        let mut parser = Parser::new(b"$-1\r\n".as_ref());
        assert_eq!(parser.bulk_string()?, None);
        Ok(())
    }

    #[test]
    fn parse_array() -> Result<()> {
        let mut parser = Parser::new(b"*3\r\n:1\r\n:2\r\n:3\r\n".as_ref());
        let vec = Vec::<i64>::deserialize(&mut parser)?;
        assert_eq!(vec, &[1, 2, 3]);
        Ok(())
    }

    #[test]
    fn parse_null_array() -> Result<()> {
        let mut parser = Parser::new(b"*-1\r\n".as_ref());
        assert_eq!(Value::deserialize(&mut parser)?, Value::Null);
        Ok(())
    }
}
