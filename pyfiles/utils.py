import argparse
import sys
from dataclasses import dataclass, fields, MISSING
from typing import List


# JobArgs should be moved to the shared library
# Different jobs have different job arguments, but once we move it to the shared library,
# we can simply fill in the fields and the arguments can be automatically parsed as an object
@dataclass
class JobArgs:
    input_path: str
    output_path: str

    @classmethod
    def parse_args(cls, args: List = None) -> __qualname__:
        args = sys.argv if args is None else args
        parser = argparse.ArgumentParser()
        for fld in fields(cls):
            is_required = fld.default is MISSING and fld.default_factory is MISSING
            parser.add_argument(f"--{fld.name}", required=is_required)
        known_args, _ = parser.parse_known_args(args)

        arg_values = {}
        for fld in fields(cls):
            value = getattr(known_args, fld.name)
            if value is not None:
                arg_values[fld.name] = value

        return JobArgs(**arg_values)
