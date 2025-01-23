from dataclasses import dataclass

from ampere.cli.common import CLIEnvironment


@dataclass
class State:
    env: CLIEnvironment = CLIEnvironment.prod
