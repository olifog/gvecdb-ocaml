
# gvecdb - A Hybrid Graph-Vector Database in OCaml

This is a Part II project for the Computer Science Tripos at the University of Cambridge. Please see the [proposal doc](proposal.pdf) for full context!

## Project Structure

- `lib/` - core library
  - `types.ml` - core type definitions
  - `keys.ml` - key encoding/decoding for LMDB
  - `store.ml` - low-level LMDB operations and string interning
  - `props_capnp.ml/.mli` - property operations using CapnProto
  - `gvecdb_internal.capnp` - internal wrapper schema for preserving metadata
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
- zero-copy reads from memory-mapped storage
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

module SchemaMod = Schemas.Make(Capnp.BytesMessage)

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
    SchemaMod.Builder.Person.name_set builder "Alice";
    SchemaMod.Builder.Person.age_set_int_exn builder 30;
    SchemaMod.Builder.Person.email_set builder "alice@example.com")
  SchemaMod.Builder.Person.init_root
  SchemaMod.Builder.Person.to_message;

(* get properties with zero-copy reads *)
let name = Gvecdb.get_node_props_capnp db alice "person"
  SchemaMod.Reader.Person.of_message
  SchemaMod.Reader.Person.name_get in
Printf.printf "Name: %s\n" name;

(* create edges *)
let edge = Gvecdb.create_edge db "knows" alice bob in

(* set edge properties *)
Gvecdb.set_edge_props_capnp db edge "knows"
  (fun builder ->
    SchemaMod.Builder.Knows.since_set builder 2020L;
    SchemaMod.Builder.Knows.strength_set builder 0.9)
  SchemaMod.Builder.Knows.init_root
  SchemaMod.Builder.Knows.to_message;

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
