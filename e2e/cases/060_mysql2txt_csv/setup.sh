# txtfilewriter requires path to be an existing directory: TxtFileWriter.Job.split
# does Objects.requireNonNull(dir.list()) before writing anything.
rm -rf "$E2E_CASE_OUT"
mkdir -p "$E2E_CASE_OUT"
