use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use byteorder::{ByteOrder, LittleEndian, BigEndian};

/// Deserialize an Odometry message using message-level parsing in Rust
#[pyfunction]
pub fn deserialize_odometry<'py>(py: Python<'py>, data: &[u8]) -> PyResult<PyObject> {
    if data.len() < 4 {
        return Err(PyValueError::new_err("Data too short for CDR header"));
    }

    let is_little_endian = data[1] != 0;
    let payload = &data[4..]; // Skip CDR header
    let mut pos = 0;

    // Helper macros for reading with proper alignment
    macro_rules! align {
        ($size:expr) => {
            if pos % $size > 0 {
                pos += $size - (pos % $size);
            }
        };
    }

    macro_rules! read_i32 {
        () => {{
            align!(4);
            if pos + 4 > payload.len() {
                return Err(PyValueError::new_err("Unexpected end of data"));
            }
            let val = if is_little_endian {
                LittleEndian::read_i32(&payload[pos..pos + 4])
            } else {
                BigEndian::read_i32(&payload[pos..pos + 4])
            };
            pos += 4;
            val
        }};
    }

    macro_rules! read_u32 {
        () => {{
            align!(4);
            if pos + 4 > payload.len() {
                return Err(PyValueError::new_err("Unexpected end of data"));
            }
            let val = if is_little_endian {
                LittleEndian::read_u32(&payload[pos..pos + 4])
            } else {
                BigEndian::read_u32(&payload[pos..pos + 4])
            };
            pos += 4;
            val
        }};
    }

    macro_rules! read_f64 {
        () => {{
            align!(8);
            if pos + 8 > payload.len() {
                return Err(PyValueError::new_err("Unexpected end of data"));
            }
            let val = if is_little_endian {
                LittleEndian::read_f64(&payload[pos..pos + 8])
            } else {
                BigEndian::read_f64(&payload[pos..pos + 8])
            };
            pos += 8;
            val
        }};
    }

    macro_rules! read_string {
        () => {{
            let len = read_u32!() as usize;
            if len <= 1 {
                if pos + len > payload.len() {
                    return Err(PyValueError::new_err("Unexpected end of data"));
                }
                pos += len;
                String::new()
            } else {
                if pos + len > payload.len() {
                    return Err(PyValueError::new_err("Unexpected end of data"));
                }
                let s = String::from_utf8_lossy(&payload[pos..pos + len - 1]).to_string();
                pos += len;
                s
            }
        }};
    }

    // Parse all fields
    let header_sec = read_i32!();
    let header_nanosec = read_u32!();
    let frame_id = read_string!();
    let child_frame_id = read_string!();

    let point_x = read_f64!();
    let point_y = read_f64!();
    let point_z = read_f64!();

    let quat_x = read_f64!();
    let quat_y = read_f64!();
    let quat_z = read_f64!();
    let quat_w = read_f64!();

    let mut pose_cov = Vec::with_capacity(36);
    for _ in 0..36 {
        pose_cov.push(read_f64!());
    }

    let linear_x = read_f64!();
    let linear_y = read_f64!();
    let linear_z = read_f64!();

    let angular_x = read_f64!();
    let angular_y = read_f64!();
    let angular_z = read_f64!();

    let mut twist_cov = Vec::with_capacity(36);
    for _ in 0..36 {
        twist_cov.push(read_f64!());
    }

    // Call Python constructor
    let constructor = py.import_bound("pybag.message_level_deserialize")?.getattr("construct_odometry_from_rust")?;

    let result = constructor.call1((
        header_sec,
        header_nanosec,
        frame_id,
        child_frame_id,
        point_x, point_y, point_z,
        quat_x, quat_y, quat_z, quat_w,
        pose_cov,
        linear_x, linear_y, linear_z,
        angular_x, angular_y, angular_z,
        twist_cov,
    ))?;

    Ok(result.into())
}
