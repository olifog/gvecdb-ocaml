(lang dune 3.17)

(name gvecdb)

(generate_opam_files true)

(source
 (github olifog/gvecdb-ocaml))

(authors "Oliver Fogelin <oliverfogelin@gmail.com>")

(maintainers "Oliver Fogelin <oliverfogelin@gmail.com>")

(license MIT)

(documentation https://github.com/olifog/gvecdb-ocaml)

(package
 (name gvecdb)
 (synopsis "A graph vector database")
 (description "A graph vector database")
 (depends
   ocaml
   dune-configurator
   capnp
   conf-capnproto
   bigstringaf
   (alcotest :with-test)
   (bisect_ppx :with-test)
 )
 (tags
  ("graph" "vector" "database")))
