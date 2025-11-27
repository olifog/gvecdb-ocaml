# gvecdb - A Hybrid Graph-Vector Database in OCaml

[![CI](https://github.com/olifog/gvecdb-ocaml/actions/workflows/ci.yml/badge.svg)](https://github.com/olifog/gvecdb-ocaml/actions/workflows/ci.yml)

This is a Part II project for the Computer Science Tripos at the University of Cambridge. Please see the [proposal doc](proposal.pdf) for full context!

## Project Structure

- `lib/` - core library
  - `types.ml` - core type definitions and database handle
  - `keys.ml` - bigstring key encoding/decoding for LMDB
  - `store.ml` - low-level LMDB operations
  - `bigstring_storage.ml` - MessageStorage implementation for bigstrings
  - `bigstring_message.ml` - CapnProto message backed by bigstrings
  - `props_capnp.ml/.mli` - property operations using CapnProto
  - `gvecdb.ml/.mli` - public API
- `bin/` - example executables
  - `example.ml` - full working example demonstrating the API
  - `main.ml` - CLI tool (placeholder)
- `test/` - unit tests
- `test_schemas/` - example CapnProto schemas for testing
- `reports/` - weekly progress reports and design decisions
- `vendor/` - vendored dependencies (ocaml-lmdb)

## Quick Start

First, make sure LMDB is installed on your system

### Build

```bash
dune build
```

### Run Example

```bash
dune exec bin/example.exe
```

This creates a simple graph with person nodes and knows/likes edges, demonstrating:

- creating nodes with string type names
- creating edges with string type names
- setting and getting properties using CapnProto schemas
- querying edges and getting full edge information (type, src, dst)
- string interning happens automatically under the hood
- persistence across runs

### Use as a Library

First, define your schemas in CapnProto format (e.g., `schemas.capnp`):

```capnp
struct Person {
  name @0 :Text;
  age @1 :UInt32;
  email @2 :Text;
}

struct Knows {
  since @0 :Int64;
  strength @1 :Float32;
}
```

Then compile them in your dune file and use in OCaml:

```ocaml
(* in your dune file: (libraries gvecdb capnp) *)

(* BytesMessage for building, Bigstring_message for reading *)
module SchemaBuilder = Schemas.Make(Capnp.BytesMessage)
module SchemaReader = Schemas.Make(Gvecdb.Bigstring_message)

let db = Gvecdb.create "/path/to/db" in

(* register your schemas *)
Gvecdb.register_node_schema_capnp db "person" 0xYOURSCHEMAID;
Gvecdb.register_edge_schema_capnp db "knows" 0xYOURSCHEMAID;

(* create nodes *)
let alice = Gvecdb.create_node db "person" in
let bob = Gvecdb.create_node db "person" in

(* set properties using CapnProto builder *)
Gvecdb.set_node_props_capnp db alice "person"
  (fun builder ->
    SchemaBuilder.Builder.Person.name_set builder "Alice";
    SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
    SchemaBuilder.Builder.Person.email_set builder "alice@example.com")
  SchemaBuilder.Builder.Person.init_root
  SchemaBuilder.Builder.Person.to_message;

(* get properties - reads directly from mmap *)
let name = Gvecdb.get_node_props_capnp db alice
  SchemaReader.Reader.Person.of_message
  SchemaReader.Reader.Person.name_get in
Printf.printf "Name: %s\n" name;

(* create edges *)
let edge = Gvecdb.create_edge db "knows" alice bob in

(* set edge properties *)
Gvecdb.set_edge_props_capnp db edge "knows"
  (fun builder ->
    SchemaBuilder.Builder.Knows.since_set builder 2020L;
    SchemaBuilder.Builder.Knows.strength_set builder 0.9)
  SchemaBuilder.Builder.Knows.init_root
  SchemaBuilder.Builder.Knows.to_message;

(* query edges - returns edge_info records with full details *)
let edges = Gvecdb.get_outbound_edges db alice in
List.iter (fun edge_info ->
  Printf.printf "edge %Ld: [%s] %Ld -> %Ld\n"
    edge_info.id edge_info.edge_type edge_info.src edge_info.dst
) edges;

(* edge metadata is preserved even after setting properties *)
match Gvecdb.get_edge_info db edge with
| Some info -> Printf.printf "Found: %Ld -[%s]-> %Ld\n" 
                 info.src info.edge_type info.dst
| None -> print_endline "Edge not found";

Gvecdb.close db
```

## Testing

```bash
dune runtest
```

### Coverage

To generate a coverage report locally:

```bash
# Run tests with coverage instrumentation
mkdir -p _coverage
dune build --instrument-with bisect_ppx
BISECT_FILE=$PWD/_coverage/bisect dune exec test/test_basic.exe
BISECT_FILE=$PWD/_coverage/bisect dune exec test/test_transactions.exe
BISECT_FILE=$PWD/_coverage/bisect dune exec test/test_zerocopy.exe
BISECT_FILE=$PWD/_coverage/bisect dune exec test/test_keys.exe
BISECT_FILE=$PWD/_coverage/bisect dune exec test/test_adjacency.exe

# Generate reports
bisect-ppx-report summary --coverage-path _coverage
bisect-ppx-report html --coverage-path _coverage -o _coverage/html
# Open _coverage/html/index.html in your browser
```
