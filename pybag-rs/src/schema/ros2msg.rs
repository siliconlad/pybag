//! ROS2 message schema parser.

use crate::error::{PybagError, Result};
use crate::schema::types::*;
use std::collections::HashMap;

/// Parser for ROS2 message schemas.
pub struct Ros2MsgParser {
    builtin_schemas: HashMap<String, Schema>,
}

impl Ros2MsgParser {
    pub fn new() -> Self {
        let mut builtin_schemas = HashMap::new();

        // builtin_interfaces/Time
        let mut time_schema = Schema::new("builtin_interfaces/Time".to_string());
        time_schema.fields.push(SchemaField {
            name: "sec".to_string(),
            field_type: FieldType::Primitive(PrimitiveType::Int32),
            default_value: None,
        });
        time_schema.fields.push(SchemaField {
            name: "nanosec".to_string(),
            field_type: FieldType::Primitive(PrimitiveType::Uint32),
            default_value: None,
        });
        builtin_schemas.insert("builtin_interfaces/Time".to_string(), time_schema);

        // builtin_interfaces/Duration
        let mut duration_schema = Schema::new("builtin_interfaces/Duration".to_string());
        duration_schema.fields.push(SchemaField {
            name: "sec".to_string(),
            field_type: FieldType::Primitive(PrimitiveType::Int32),
            default_value: None,
        });
        duration_schema.fields.push(SchemaField {
            name: "nanosec".to_string(),
            field_type: FieldType::Primitive(PrimitiveType::Uint32),
            default_value: None,
        });
        builtin_schemas.insert("builtin_interfaces/Duration".to_string(), duration_schema);

        Self { builtin_schemas }
    }

    /// Parse a ROS2 message schema.
    pub fn parse(&self, name: &str, data: &[u8]) -> Result<(Schema, HashMap<String, Schema>)> {
        let text = std::str::from_utf8(data)
            .map_err(|e| PybagError::SchemaParseError(format!("Invalid UTF-8: {}", e)))?;

        let package_name = name.split('/').next().unwrap_or("");

        // Remove comments and empty lines
        let lines: Vec<&str> = text
            .lines()
            .map(|line| Self::remove_inline_comment(line))
            .filter(|line| !line.is_empty())
            .collect();

        let cleaned = lines.join("\n");

        // Split by delimiter
        let parts: Vec<&str> = cleaned.split("================================================================================").collect();

        // Parse main schema
        let main_fields = parts[0].trim();
        let main_schema = self.parse_message_fields(name, main_fields, package_name)?;

        // Parse sub-schemas
        let mut sub_schemas: HashMap<String, Schema> = HashMap::new();
        for part in parts.iter().skip(1) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let lines: Vec<&str> = part.lines().collect();
            if lines.is_empty() {
                continue;
            }
            // First line should be "MSG: package/Type"
            let first_line = lines[0].trim();
            if !first_line.starts_with("MSG: ") {
                continue;
            }
            let sub_name = &first_line[5..];
            let sub_fields = lines[1..].join("\n");
            let sub_package = sub_name.split('/').next().unwrap_or("");
            let sub_schema = self.parse_message_fields(sub_name, &sub_fields, sub_package)?;
            sub_schemas.insert(sub_name.to_string(), sub_schema);
        }

        // Add builtin schemas if referenced
        for (builtin_name, builtin_schema) in &self.builtin_schemas {
            if !sub_schemas.contains_key(builtin_name) && text.contains(builtin_name) {
                sub_schemas.insert(builtin_name.clone(), builtin_schema.clone());
            }
        }

        Ok((main_schema, sub_schemas))
    }

    fn parse_message_fields(&self, name: &str, text: &str, package_name: &str) -> Result<Schema> {
        let mut schema = Schema::new(name.to_string());

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse field: TYPE NAME [DEFAULT]
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }

            let type_str = parts[0];
            let name_and_maybe_value = parts[1];

            // Check if this is a constant (NAME=VALUE or NAME = VALUE)
            if name_and_maybe_value.contains('=') || (parts.len() > 2 && parts[2].starts_with('=')) {
                // Parse constant
                let (const_name, const_value) = if name_and_maybe_value.contains('=') {
                    let split: Vec<&str> = name_and_maybe_value.splitn(2, '=').collect();
                    (split[0], split.get(1).map(|s| s.to_string()).unwrap_or_default())
                } else {
                    // NAME = VALUE format
                    let value = if parts.len() > 3 {
                        parts[3..].join(" ")
                    } else if parts.len() > 2 && parts[2].len() > 1 {
                        parts[2][1..].to_string() // Skip the '='
                    } else {
                        String::new()
                    };
                    (name_and_maybe_value, value)
                };

                let field_type = self.parse_field_type(type_str, package_name)?;
                let value = self.parse_value(&field_type, &const_value)?;

                schema.constants.push(SchemaConstant {
                    name: const_name.to_string(),
                    field_type,
                    value,
                });
            } else {
                // Parse regular field
                let field_type = self.parse_field_type(type_str, package_name)?;
                let default_value = if parts.len() > 2 {
                    let default_str = parts[2..].join(" ");
                    Some(self.parse_value(&field_type, &default_str)?)
                } else {
                    None
                };

                schema.fields.push(SchemaField {
                    name: name_and_maybe_value.to_string(),
                    field_type,
                    default_value,
                });
            }
        }

        Ok(schema)
    }

    fn parse_field_type(&self, type_str: &str, package_name: &str) -> Result<FieldType> {
        // Handle arrays and sequences
        if let Some(bracket_pos) = type_str.find('[') {
            let element_str = &type_str[..bracket_pos];
            let length_str = &type_str[bracket_pos + 1..type_str.len() - 1];
            let element_type = self.parse_field_type(element_str, package_name)?;

            if length_str.is_empty() {
                return Ok(FieldType::Sequence {
                    element_type: Box::new(element_type),
                    max_length: None,
                });
            }

            if length_str.starts_with("<=") {
                let max_len: usize = length_str[2..].parse()
                    .map_err(|_| PybagError::SchemaParseError(format!("Invalid bounded length: {}", length_str)))?;
                return Ok(FieldType::Array {
                    element_type: Box::new(element_type),
                    length: max_len,
                    is_bounded: true,
                });
            }

            let length: usize = length_str.parse()
                .map_err(|_| PybagError::SchemaParseError(format!("Invalid array length: {}", length_str)))?;
            return Ok(FieldType::Array {
                element_type: Box::new(element_type),
                length,
                is_bounded: false,
            });
        }

        // Handle string types
        if type_str.starts_with("string") {
            if let Some(bound_start) = type_str.find("<=") {
                let max_len: usize = type_str[bound_start + 2..].parse()
                    .map_err(|_| PybagError::SchemaParseError(format!("Invalid string bound: {}", type_str)))?;
                return Ok(FieldType::String(StringType {
                    is_wide: false,
                    max_length: Some(max_len),
                }));
            }
            return Ok(FieldType::String(StringType {
                is_wide: false,
                max_length: None,
            }));
        }

        if type_str.starts_with("wstring") {
            if let Some(bound_start) = type_str.find("<=") {
                let max_len: usize = type_str[bound_start + 2..].parse()
                    .map_err(|_| PybagError::SchemaParseError(format!("Invalid wstring bound: {}", type_str)))?;
                return Ok(FieldType::String(StringType {
                    is_wide: true,
                    max_length: Some(max_len),
                }));
            }
            return Ok(FieldType::String(StringType {
                is_wide: true,
                max_length: None,
            }));
        }

        // Handle primitive types
        if let Some(prim) = PrimitiveType::from_str(type_str) {
            return Ok(FieldType::Primitive(prim));
        }

        // Handle complex types
        let mut full_name = type_str.to_string();
        if full_name == "Header" {
            full_name = "std_msgs/Header".to_string();
        } else if !full_name.contains('/') {
            full_name = format!("{}/{}", package_name, type_str);
        }

        Ok(FieldType::Complex { type_name: full_name })
    }

    fn parse_value(&self, field_type: &FieldType, value_str: &str) -> Result<FieldValue> {
        let value_str = value_str.trim();

        match field_type {
            FieldType::Primitive(prim) => match prim {
                PrimitiveType::Bool => {
                    let v = value_str == "true" || value_str == "True" || value_str == "1";
                    Ok(FieldValue::Bool(v))
                }
                PrimitiveType::Int8 | PrimitiveType::Int16 | PrimitiveType::Int32 | PrimitiveType::Int64 => {
                    let v: i64 = value_str.parse()
                        .map_err(|_| PybagError::SchemaParseError(format!("Invalid int: {}", value_str)))?;
                    Ok(FieldValue::Int(v))
                }
                PrimitiveType::Uint8 | PrimitiveType::Uint16 | PrimitiveType::Uint32 | PrimitiveType::Uint64 | PrimitiveType::Byte => {
                    let v: u64 = value_str.parse()
                        .map_err(|_| PybagError::SchemaParseError(format!("Invalid uint: {}", value_str)))?;
                    Ok(FieldValue::Uint(v))
                }
                PrimitiveType::Float32 | PrimitiveType::Float64 => {
                    let v: f64 = value_str.parse()
                        .map_err(|_| PybagError::SchemaParseError(format!("Invalid float: {}", value_str)))?;
                    Ok(FieldValue::Float(v))
                }
                PrimitiveType::Char => {
                    let c = value_str.chars().next().unwrap_or('\0');
                    Ok(FieldValue::Uint(c as u64))
                }
            },
            FieldType::String(_) => {
                let s = value_str.trim_matches('"').trim_matches('\'').to_string();
                Ok(FieldValue::String(s))
            }
            FieldType::Array { element_type, .. } | FieldType::Sequence { element_type, .. } => {
                // Parse array literal: [1, 2, 3]
                if !value_str.starts_with('[') || !value_str.ends_with(']') {
                    return Err(PybagError::SchemaParseError(format!("Invalid array: {}", value_str)));
                }
                let inner = &value_str[1..value_str.len() - 1];
                let elements: Vec<FieldValue> = inner
                    .split(',')
                    .map(|s| self.parse_value(element_type, s.trim()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FieldValue::Array(elements))
            }
            FieldType::Complex { .. } => {
                Err(PybagError::SchemaParseError("Complex types cannot have default values".to_string()))
            }
        }
    }

    fn remove_inline_comment(line: &str) -> &str {
        let mut in_single = false;
        let mut in_double = false;

        for (i, c) in line.char_indices() {
            match c {
                '\'' if !in_double => in_single = !in_single,
                '"' if !in_single => in_double = !in_double,
                '#' if !in_single && !in_double => return line[..i].trim(),
                _ => {}
            }
        }
        line.trim()
    }
}

impl Default for Ros2MsgParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_schema() {
        let parser = Ros2MsgParser::new();
        let schema_data = b"float64 x\nfloat64 y\nfloat64 z\n";
        let (schema, _) = parser.parse("geometry_msgs/msg/Point", schema_data).unwrap();

        assert_eq!(schema.name, "geometry_msgs/msg/Point");
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].name, "x");
        assert_eq!(schema.fields[1].name, "y");
        assert_eq!(schema.fields[2].name, "z");
    }

    #[test]
    fn test_parse_with_constants() {
        let parser = Ros2MsgParser::new();
        let schema_data = b"byte OK=0\nbyte WARN=1\nbyte ERROR=2\nbyte level\nstring message\n";
        let (schema, _) = parser.parse("diagnostic_msgs/msg/DiagnosticStatus", schema_data).unwrap();

        assert_eq!(schema.constants.len(), 3);
        assert_eq!(schema.fields.len(), 2);
    }
}
