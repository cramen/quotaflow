#!/usr/bin/env python3
"""Record an explicit trusted-policy exception; never claim performance passed."""
import argparse
import json
from pathlib import Path
from verify_release_evidence import performance_exception


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = performance_exception(json.loads(args.candidate.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as output:
        output.write(json.dumps(result, indent=2) + '\n')
    print('Performance WAIVED by explicit release policy; performance is not certified')


if __name__ == '__main__':
    main()
