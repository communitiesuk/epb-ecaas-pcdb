This directory contains files copied verbatim from the FHS repository and used as base files for merging the
"transformed" snippets onto before checking that they conform to the schema. There is nothing special about the choice
of files other than that they are compatible with respective snippets - e.g. the fancoil snippet requires a base file
that uses fancoil and no radiators in order to ensure that other invariants in the file hold that are not directly to do
with this snippet (in the case of fancoils, the presence of no `thermal_mass` field).