# dependencies
require "cgi"
require "pg"
require "thor"
require "time"
require "uri"

# modules
require "pgslice/helpers"
require "pgslice/table"
require "pgslice/version"

# commands
require "pgslice/cli"
require "pgslice/cli/add_partitions"
require "pgslice/cli/analyze"
require "pgslice/cli/fill"
require "pgslice/cli/prep"
require "pgslice/cli/swap"
require "pgslice/cli/unprep"
require "pgslice/cli/unswap"
# custom command
require "pgslice/cli/slice_default_partition"
