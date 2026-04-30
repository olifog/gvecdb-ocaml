@0xb8e3f4a1c2d5e6f7;

struct Paper {
  title @0 :Text;
  abstract @1 :Text;
  year @2 :UInt16;
  arxivId @3 :Text;
  categories @4 :Text;
  doi @5 :Text;
  journalRef @6 :Text;
  submittedDate @7 :Text;
  pageCount @8 :UInt16;
  figureCount @9 :UInt16;
  versionCount @10 :UInt8;
  comments @11 :Text;
}

struct Author {
  name @0 :Text;
  paperCount @1 :UInt32;
}

struct Authored {
  position @0 :UInt8;
}

struct Cites {
  context @0 :Text;
}
