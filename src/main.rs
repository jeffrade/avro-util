use std::fs::File;
use std::io::prelude::*;

use avro_rs::{from_value, types::Record, Codec, Reader, Schema, Writer};
use failure::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct SchemaExample {
    field_1: i64,
    field_2: String,
}

fn main() -> Result<(), Error> {
    let mut schema_file = File::open("avro-rs-example.avsc")?;
    let mut raw_schema = String::new();
    schema_file.read_to_string(&mut raw_schema)?;
    let schema = Schema::parse_str(&raw_schema)?;

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);
    let mut record = Record::new(writer.schema()).unwrap();

    record.put("field_1", 27i64);
    record.put("field_2", "foo");
    writer.append(record)?;

    let another_message = SchemaExample {
        field_1: 64,
        field_2: "bar".to_owned(),
    };
    writer.append_ser(another_message)?;
    writer.flush()?;

    let input: Vec<u8> = writer.into_inner();
    let reader: Reader<&[u8]> = Reader::with_schema(&schema, &input[..])?;

    for record in reader {
        println!("{:?}", from_value::<SchemaExample>(&record?).unwrap());
    }
    Ok(())
}
