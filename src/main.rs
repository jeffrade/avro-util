use std::fs::File;
use std::io::prelude::*;

use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct SchemaTest {
    a: i64,
    b: String,
}

// Contents of avro-rs-example.avsc below:
// {
//   "type": "record",
//   "name": "test",
//   "fields": [
//     {"name": "a", "type": "long", "default": 42},
//     {"name": "b", "type": "string"}
//   ]
// }
fn main() -> Result<(), Error> {
    let mut schema_file = File::open("avro-rs-example.avsc")?;
    let mut raw_schema = String::new();
    schema_file.read_to_string(&mut raw_schema)?;

    let schema = Schema::parse_str(&raw_schema)?;

    println!("{:?}", schema);

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("a", 27i64);
    record.put("b", "foo");

    writer.append(record)?;

    let test_message = SchemaTest {
        a: 27,
        b: "foo".to_owned(),
    };

    writer.append_ser(test_message)?;

    writer.flush()?;

    let input = writer.into_inner();
    let reader = Reader::with_schema(&schema, &input[..])?;

    for record in reader {
        println!("{:?}", from_value::<SchemaTest>(&record?));
    }
    Ok(())
}