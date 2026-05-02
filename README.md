# gvecdb - A Hybrid Graph-Vector Database in OCaml

[![CI](https://github.com/olifog/gvecdb-ocaml/actions/workflows/ci.yml/badge.svg)](https://github.com/olifog/gvecdb-ocaml/actions/workflows/ci.yml)

This is a Part II project for the Computer Science Tripos at the University of Cambridge. Please see the [proposal doc](proposal.pdf) for full context!

## Overview

gvecdb is a hybrid graph-vector database combining:

- **LMDB** for durable graph storage (nodes, edges, adjacency indices, properties)
- **HNSW** approximate nearest neighbor index with MVCC persistence
- **Cap'n Proto** for schema-aware zero-copy property storage and RPC
- **Native float32 SIMD** via OxCaml's unboxed types and AVX intrinsics

It can be used as an embedded OCaml library or as a standalone Cap'n Proto RPC server.

## Project Structure

- `lib/` - core library
  - `gvecdb.ml/.mli` - public API
  - `types.ml` - core type definitions and database handle
  - `keys.ml` - bigstring key encoding/decoding for LMDB
  - `store.ml` - low-level LMDB operations
  - `float32_vec.ml` - SIMD float32 distance computation
  - `vector_file.ml` - append-only mmap'd vector storage
  - `hnsw.ml` - HNSW parameters
  - `hnsw_page.ml` - HNSW node page layout and serialization
  - `hnsw_mvcc.ml` - HNSW MVCC persistence layer
  - `knn.ml` - brute-force k-NN search
  - `schema_registry.ml` - runtime schema registration and persistence
  - `dynamic_reader.ml` - read Cap'n Proto fields by name at runtime
  - `filter.ml` - property-based edge filtering
  - `props_capnp.ml` - schema metadata storage
  - `bitset.ml`, `int_heap.ml`, `int_topk.ml` - data structures for search
  - `msync.ml` / `msync_stubs.c` - mmap flush bindings
- `server/` - Cap'n Proto RPC server
  - `gvecdb_api.capnp` - RPC schema
  - `gvecdb_service.ml` - server implementation
  - `gvecdb_client.ml` - client wrapper
  - `main.ml` - server entry point
- `test/` - test suite
- `bench/` - benchmarks (ANN recall/QPS, insertion, graph ops, crash recovery, concurrency)
- `scripts/` - benchmark runners, dataset downloaders, plotting
- `test_schemas/` - Cap'n Proto schemas for testing
- `reports/` - progress reports and design decisions
- `vendor/` - vendored dependencies (ocaml-lmdb)
- `demo/` - arXiv Explorer full-stack demo app

## Quick Start

### Prerequisites

**OxCaml**: 

```bash
opam switch create 5.2.0+ox ocaml-variants.5.2.0+ox
```

**System dependencies** (C libraries):

```bash
# Ubuntu/Debian
sudo apt install liblmdb-dev capnproto pkg-config

# macOS
brew install lmdb capnp pkg-config

# Arch
sudo pacman -S lmdb capnproto pkgconf
```

**OCaml dependencies**:

```bash
opam install . --deps-only --with-test -y
```

### Clone

```bash
git clone --recurse-submodules https://github.com/olifog/gvecdb-ocaml.git
cd gvecdb-ocaml

# If already cloned without submodules:
git submodule update --init
```

### Build

```bash
dune build
```

### Run Tests

```bash
dune runtest
```

### Run Server

```bash
dune exec server/main.exe -- --db /path/to/my.db
```

## Use as a Library

Define your schemas in Cap'n Proto format (e.g., `schemas.capnp`):

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

Compile them in your dune file and use in OCaml:

```ocaml
(* in your dune file: (libraries gvecdb capnp) *)

module SchemaBuilder = Schemas.Make(Capnp.BytesMessage)

let () =
  let db = match Gvecdb.create "/path/to/db" with
    | Ok db -> db
    | Error e -> failwith (Gvecdb.Error.to_string e)
  in

  (* register schemas for dynamic field access and filtering *)
  let _ = Gvecdb.register_schema_from_capnp db
    ~kind:Gvecdb.Schema_registry.NodeSchemaKind
    ~type_name:"person" ~capnp_path:"schemas.capnp"
    ~struct_name:"Person" () in

  (* create nodes *)
  let alice = match Gvecdb.create_node db "person" with
    | Ok id -> id | Error e -> failwith (Gvecdb.Error.to_string e) in
  let bob = match Gvecdb.create_node db "person" with
    | Ok id -> id | Error e -> failwith (Gvecdb.Error.to_string e) in

  (* set properties as serialized Cap'n Proto bytes *)
  let builder = SchemaBuilder.Builder.Person.init_root () in
  SchemaBuilder.Builder.Person.name_set builder "Alice";
  SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
  let msg = SchemaBuilder.Builder.Person.to_message builder in
  let bs = (* serialize msg to bigstring *) in
  ignore (Gvecdb.set_node_props db alice "person" bs);

  (* create edges *)
  ignore (Gvecdb.create_edge db "knows" alice bob);

  (* query edges *)
  let edges = match Gvecdb.get_outbound_edges db alice () with
    | Ok es -> es | Error _ -> [] in
  List.iter (fun (e : Gvecdb.edge_info) ->
    Printf.printf "edge %Ld: [%s] %Ld -> %Ld\n"
      e.id e.edge_type e.src e.dst
  ) edges;

  (* add vectors and search *)
  ignore (Gvecdb.with_transaction db (fun txn ->
    let vec = Gvecdb.Float32_vec.of_array [| 1.0; 0.5; 0.3 |] in
    ignore (Gvecdb.create_vector db ~txn Node alice "embedding" vec)));

  (* read fields dynamically by name *)
  (match Gvecdb.read_node_field db alice "age" with
   | Ok (Gvecdb.Dynamic_reader.V_uint32 age) ->
       Printf.printf "age: %ld\n" age
   | _ -> ());

  Gvecdb.close db
```

## Coverage

To generate a coverage report locally:

```bash
mkdir -p _coverage
dune build --instrument-with bisect_ppx
for test in test_basic test_vectors test_hnsw test_hnsw_mvcc \
            test_adjacency test_transactions test_schema_filter \
            test_integration; do
  BISECT_FILE=$PWD/_coverage/bisect dune exec test/${test}.exe
done

bisect-ppx-report summary --coverage-path _coverage
bisect-ppx-report html --coverage-path _coverage -o _coverage/html
```
