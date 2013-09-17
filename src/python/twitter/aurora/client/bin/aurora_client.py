from twitter.aurora.client.base import generate_terse_usage
from twitter.common import app
from twitter.common.log.options import LogOptions

# These are are side-effecting imports in that they register commands via
# app.command.  This is a poor code practice and should be fixed long-term
# with the creation of twitter.common.cli that allows for argparse-style CLI
# composition.
from twitter.aurora.client.commands import (
    core,
    run,
    ssh,
)
from twitter.aurora.client.options import add_verbosity_options

app.register_commands_from(core, run, ssh)
add_verbosity_options()


def main():
  app.help()


LogOptions.set_stderr_log_level('INFO')
LogOptions.disable_disk_logging()
app.set_name('aurora-client')
app.set_usage(generate_terse_usage())
app.main()
