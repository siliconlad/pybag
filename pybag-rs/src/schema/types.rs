//! Schema type definitions.

use std::collections::HashMap;

/// Primitive type names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimitiveType {
    Bool,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float32,
    Float64,
    Byte,
    Char,
}

impl PrimitiveType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "bool" => Some(Self::Bool),
            "int8" => Some(Self::Int8),
            "uint8" => Some(Self::Uint8),
            "int16" => Some(Self::Int16),
            "uint16" => Some(Self::Uint16),
            "int32" => Some(Self::Int32),
            "uint32" => Some(Self::Uint32),
            "int64" => Some(Self::Int64),
            "uint64" => Some(Self::Uint64),
            "float32" => Some(Self::Float32),
            "float64" => Some(Self::Float64),
            "byte" | "octet" => Some(Self::Byte),
            "char" => Some(Self::Char),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int8 => "int8",
            Self::Uint8 => "uint8",
            Self::Int16 => "int16",
            Self::Uint16 => "uint16",
            Self::Int32 => "int32",
            Self::Uint32 => "uint32",
            Self::Int64 => "int64",
            Self::Uint64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Byte => "byte",
            Self::Char => "char",
        }
    }
}

/// String type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringType {
    pub is_wide: bool,
    pub max_length: Option<usize>,
}

/// Field type.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Primitive(PrimitiveType),
    String(StringType),
    Array {
        element_type: Box<FieldType>,
        length: usize,
        is_bounded: bool,
    },
    Sequence {
        element_type: Box<FieldType>,
        max_length: Option<usize>,
    },
    Complex {
        type_name: String,
    },
}

/// Schema field.
#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    pub field_type: FieldType,
    pub default_value: Option<FieldValue>,
}

/// Schema constant.
#[derive(Debug, Clone)]
pub struct SchemaConstant {
    pub name: String,
    pub field_type: FieldType,
    pub value: FieldValue,
}

/// Field value for defaults and constants.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    String(String),
    Array(Vec<FieldValue>),
}

/// Parsed schema.
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub fields: Vec<SchemaField>,
    pub constants: Vec<SchemaConstant>,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Self {
            name,
            fields: Vec::new(),
            constants: Vec::new(),
        }
    }
}
